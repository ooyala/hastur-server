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

    # pre-declare the class structure to make introspection work
    class Seq;                    end
    class Envelope;               end
    class Base;                   end
    class Stat            < Base; end
    class Error           < Base; end
    class Rawdata         < Base; end
    class PluginExec      < Base; end
    class PluginResult    < Base; end
    class RegisterClient  < Base; end
    class RegisterPlugin  < Base; end
    class RegisterService < Base; end

    # every message has a route/method embedded, make sure they're valid
    # and let apps shunt everything else to :error
    # also handy for mapping a symbol/string to the right class consistently
    ROUTES = {
      :stat             => Stat,
      :error            => Error,
      :rawdata          => Rawdata,
      :plugin_exec      => PluginExec,
      :plugin_result    => PluginResult,
      :register_client  => RegisterClient,
      :register_plugin  => RegisterPlugin,
      :register_service => RegisterService,
    }

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
        raise ArgumentError.new("Invalid route '#{route}'") unless ROUTES.has_key?(route)

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

      if klass = ROUTES[envelope.route]
        return klass.new(:payload => payload, :envelope => envelope)
      else
        raise ArgumentError.new "Invalid route '#{route}'"
      end
    end

    #
    # base class for the various Hastur::Message types
    #
    class Base
      attr_reader :route, :envelope, :payload, :parts, :timestamp, :uptime, :sequence

      def initialize(route, envelope, payload, data)
        raise ArgumentError.new("Invalid route '#{route}'") unless ROUTES.has_key?(route)
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
    class Stat
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
    class Error
      def initialize(opts)
        data = nil

        if opts.kind_of? String
          data = {
            :error        => opts,
            :error_string => opts
          }
        elsif opts[:error]
          # might want to be more strict about the type of error, or add some
          # automatic conversions for things like exceptions
          data = {
            :error        => opts[:error],
            :error_string => opts[:error].to_s
          }
        end

        super(:error, opts[:envelope], opts[:payload], data)
      end
    end

    class Rawdata
      def initialize(opts)
        data = nil
        if opts[:rawdata]
          data = { :rawdata => opts[:rawdata] }
        end
        super(:rawdata, opts[:envelope], opts[:payload], data)
      end
    end

    class PluginExec
      def initialize(opts)
        super(:plugin_exec, opts[:envelope], opts[:payload], opts[:plugin_exec])
      end
    end

    class PluginResult
      def initialize(opts)
        super(:plugin_result, opts[:envelope], opts[:payload], opts[:plugin_result])
      end
    end

    class RegisterClient
      def initialize(opts)
        super(:register_client, opts[:envelope], opts[:payload], opts[:register_client])
      end
    end

    class RegisterService
      def initialize(opts)
        super(:register_service, opts[:envelope], opts[:payload], opts[:register_service])
      end
    end

    class RegisterPlugin
      def initialize(opts)
        super(:register_plugin, opts[:envelope], opts[:payload], opts[:register_plugin])
      end
    end
  end
end

