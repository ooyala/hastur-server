require 'ffi-rzmq'
require 'multi_json'

module Hastur
  #
  # Handle all of the busywork around Hastur messages consistently.
  #
  # Updating existing messages is not currently supported. Unless we come across a clear case where
  # it will help clean up code or improve performance measurably, it won't happen.
  #
  # Envelopes are reusable.
  #
  module Message
    # application boot time, intentionally not system boot time
    BOOT_TIME = Time.new.to_f

    # every message has a route/method embedded, make sure they're valid
    # and let apps shunt everything else to :error
    VALID_ROUTES = [
      :stat,             # Hastur::Message::Stat,
      :error,            # Hastur::Message::Error,
      :rawdata,          # Hastur::Message::Rawdata,
      :plugin_exec,      # Hastur::Message::PluginExec,
      :plugin_result,    # Hastur::Message::PluginResult,
      :register_client,  # Hastur::Message::RegisterClient,
      :register_service, # Hastur::Message::RegisterService,
      :register_plugin,  # Hastur::Message::RegisterPlugin,
    ]

    #
    # keep a single, global counter for the :sequence field
    #
    class Seq
      @counter = 0
      def self.next
        @counter+=1
      end
    end

    #
    # parsing & creating Hastur message envelopes, V1 only
    #
    class Envelope
      attr_reader :route, :version

      #
      # parse a Hastur routing envelope, usually in the multi part just ahead of the payload
      #
      def self.parse(msg)
        parts = msg.split("\n")
        Envelope.new(parts[1].to_sym, parts[2])
      end

      #
      # create a new envelope, only the route is required and acks can be enabled (default off)
      #
      def initialize(route, ack=false)
        raise ArgumentError.new("Invalid route '#{route}'") unless VALID_ROUTES.include?(route)

        @route   = route
        @version = 'v1'

        if ack == true or ack == "ack:1"
          @ack = true
        else
          @ack = false
        end
      end
      
      def ack
        @ack ? "ack:1" : "ack:0"
      end

      #
      # check whether acks are enabled on the envelope
      #
      def ack?
        @ack
      end

      #
      # stringify to the on-wire format
      #
      def to_s
        [@version, @route, ack() ].join("\n")
      end
    end

    #
    # receive a message from a ZeroMQ socket and return an appropriate Hastur::Message::* class,
    # e.g. route => :rawdata will return a Hastur::Message::Rawdata
    # 
    # object = Hastur::Message.recv(@socket)
    # object.route    # symbol for the route
    # object.envelope # Hastur::Message::Envelope
    # object.payload  # usually JSON
    # object.send(@socket)
    #
    def self.recv(socket)
      messages = []
      rc = socket.recvmsgs messages

      payload = messages[-1].copy_out_string
      messages.pop.close

      envelope = Envelope.parse messages[-1].copy_out_string
      messages.pop.close

      # TODO: flatten to a hash after looking up why I can't build said hash (compiler is whining about undefined class)
      case envelope.route
        when :stat
          return Stat.new :payload => payload, :envelope => envelope
        when :error
          return Error.new :payload => payload, :envelope => envelope
        when :rawdata
          return Rawdata.new :payload => payload, :envelope => envelope
        when :plugin_exec
          return PluginExec.new :payload => payload, :envelope => envelope
        when :plugin_result
          return PluginResult.new :payload => payload, :envelope => envelope
        when :register_client
          return RegisterClient.new :payload => payload, :envelope => envelope
        when :register_service
          return RegisterService.new :payload => payload, :envelope => envelope
        when :register_plugin
          return RegisterPlugin.new :payload => payload, :envelope => envelope
        else
          raise ArgumentError.new("Invalid route '#{route}'") unless VALID_ROUTES.include?(route)
      end
    end

    #
    # base class for the various Hastur::Message types
    #
    class Base
      attr_reader :route, :envelope, :payload, :parts, :timestamp, :uptime, :sequence

      def initialize(route, envelope, payload, data)
        raise ArgumentError.new("Invalid route '#{route}'") unless VALID_ROUTES.include?(route)
        @route = route
        @envelope = envelope.nil? ? Envelope.new(route) : envelope

        if payload.nil? and not data.nil?
          unless data.respond_to?(:to_hash)
            raise ArgumentError.new "4th argument must have a to_hash() method."
          end
          @payload = MultiJson.encode(data.to_hash)
        elsif data.nil? and not payload.nil?
          data = MultiJson.decode(payload)
        else
          raise ArgumentError.new "One or both of the 3rd/4th arguments must be set."
        end

        @timestamp = data[:timestamp] || Time.new.to_f
        @uptime    = data[:uptime]    || @timestamp - BOOT_TIME
        @sequence  = data[:sequence]  || Hastur::Message::Seq.next
      end

      #
      # send the message on a ZeroMQ socket. This is not particular about what kind of ZeroMQ socket.
      # Care is taken to try to use ZMQ::Message as-is without converting to/from strings until it's
      # necessary. Generally this only helps with router envelopes where they exist, since ZMQ::Message's
      # are generally use-once only.
      #
      def send(socket)
        messages = []

        if @parts and not @parts.empty?
          messages << @parts
          @parts.replace([]) # clear it
        end

        messages << ZMQ::Message.new(@envelope.to_s)
        payload = ZMQ::Message.new(@payload.to_s)

        # https://github.com/chuckremes/ffi-rzmq/blob/bdd0a399/lib/ffi-rzmq/socket.rb#L278
        messages.each do |p|
          rc = socket.send_and_close(p, ZMQ::SNDMORE)
          #unless ZMQ::Util.resultcode_ok?(rc)
          #  raise Hastur::ZMQReturnCodeError.new rc
          #end
        end

        socket.send_and_close(payload)
      end
    end

    #
    # s = Hastur::Message::Stat.new(:payload => json_string)
    #
    # stat = Hastur::Stat.new( ... )
    # s = Hastur::Message::Stat.new(stat)
    # 
    class Stat < Base
      def initialize(opts)
        super(:stat, opts[:envelope], opts[:payload], opts[:stat])
      end
    end

    #
    # s = Hastur::Message::Error.new :payload => json_string
    # 
    # rescue FooBar => e
    #   s = Hastur::Message::Error.new :error => e
    # end
    # 
    class Error < Base
      def initialize(opts)
        data = nil

        if opts[:error]
          # might want to be more strict about the type of error, or add some
          # automatic conversions for things like exceptions
          data = {
            :error        => error,
            :error_string => error.to_s
          }
        end

        super(:error, opts[:envelope], opts[:payload], data)
      end
    end

    class Rawdata < Base
      def initialize(opts)
        data = nil
        if opts[:rawdata]
          data = { :rawdata => opts[:rawdata] }
        end
        super(:rawdata, opts[:envelope], opts[:payload], data)
      end
    end

    class PluginExec < Base
      def initialize(opts)
        super(:plugin_exec, opts[:envelope], opts[:payload], opts[:plugin_exec])
      end
    end

    class PluginResult < Base
      def initialize(opts)
        super(:plugin_result, opts[:envelope], opts[:payload], opts[:plugin_result])
      end
    end

    class RegisterClient < Base
      def initialize(opts)
        super(:register_client, opts[:envelope], opts[:payload], opts[:register_client])
      end
    end

    class RegisterService < Base
      def initialize(opts)
        super(:register_service, opts[:envelope], opts[:payload], opts[:register_service])
      end
    end

    class RegisterPlugin < Base
      def initialize(opts)
        super(:register_plugin, opts[:envelope], opts[:payload], opts[:register_plugin])
      end
    end
  end
end

