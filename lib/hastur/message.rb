require 'ffi-rzmq'
require 'multi_json'

module Hastur
  #
  # Handle all of the busywork around Hastur messages consistently.
  #
  # Updating existing messages is not currently supported. Unless we come across a clear case where
  # it will help clean up code or improve performance measurably, it won't happen.
  #
  module Message
    # pre-declare the class structure to make introspection work
    class Seq;                    end
    class Base;                   end
    class Stat            < Base; end
    class Error           < Base; end
    class Rawdata         < Base; end
    class PluginExec      < Base; end
    class PluginResult    < Base; end
    class RegisterClient  < Base; end
    class RegisterPlugin  < Base; end
    class RegisterService < Base; end
  end

  # application boot time, intentionally not system boot time
  BOOT_TIME = Time.new.to_f

  # every message has a route/method embedded, make sure they're valid
  # and let apps shunt everything else to :error
  # also handy for mapping a symbol/string to the right class consistently
  ROUTES = {
    :stat             => Hastur::Message::Stat,
    :error            => Hastur::Message::Error,
    :rawdata          => Hastur::Message::Rawdata,
    :plugin_exec      => Hastur::Message::PluginExec,
    :plugin_result    => Hastur::Message::PluginResult,
    :register_client  => Hastur::Message::RegisterClient,
    :register_plugin  => Hastur::Message::RegisterPlugin,
    :register_service => Hastur::Message::RegisterService,
  }

  UUID_RE = /\A[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z/i

  #
  # parsing & creating Hastur envelopes, V1
  # Format:
  # field: version  route      ack      uuid         mime type
  # type:  <uint16> <char[16]> <uint16> <char[16]>   <char 32>
  # pack:  n        Z16        n        H8H4H4H4H12  a32
  #
  # Version doesn't really have to be bumped unless one of these 4 fields
  # changes type incompatibly.  For example, we can add an HMAC field on the
  # end later on and old unpacks will just ignore data at the end.
  #
  class Envelope
    VERSION = 1
    MSGBYTES = 68
    PACK = %w[ n Z16 H8 H4 H4 H4 H12 n a32 ].join
    attr_reader :version, :route, :uuid, :ack, :mime_type

    #
    # parse a Hastur routing envelope, usually in the multi part just ahead of the payload
    # it is a binary string with a version, the route (string), an ack flag (1/0), and the UUID
    #
    def self.parse(msg)
      parts = msg.unpack(PACK)
      Envelope.new({
        :version   => parts[0],
        :route     => parts[1],
        :uuid      => parts[2..6].join('-'),
        :ack       => parts[7],
        :mime_type => parts[8],
      })
    end

    #
    # pack an envelope into a binary string, ready to send on the wire
    #
    def pack
      [@version, @route.to_s, *@uuid.split(/-/), @ack, @mime_type].pack(PACK)
    end

    #
    # create a new envelope, only the route is required and acks can be enabled (default off)
    #
    def initialize(opts)
      raise ArgumentError.new(":route argument is required") unless opts[:route]
      raise ArgumentError.new("Invalid route '#{opts[:route]}'") unless ROUTES.has_key?(opts[:route].to_sym)
      raise ArgumentError.new(":uuid argument is required") unless opts[:uuid]
      raise ArgumentError.new("'#{opts[:uuid]} is not a valid UUID") unless UUID_RE.match(opts[:uuid])

      @route   = opts[:route].to_sym
      @uuid    = opts[:uuid]
      @version = opts[:version] || VERSION

      if opts[:ack].kind_of? Fixnum
        @ack = opts[:ack]
      elsif opts[:ack].kind_of? TrueClass
        @ack = 1
      else
        @ack = 0
      end

      # mime type input is ignored for now, since it should always be JSON, its presence
      # in the protocol is for future use (e.g. core files, compression, encryption)
      @mime_type = opts[:mime_type] || "application/json"
    end
    
    #
    # check whether acks are enabled on the envelope
    #
    def ack?
      @ack > 0 ? true : false
    end

    def to_s
      pack.unpack('H*')[0]
    end
  end

  #
  # A collection of classes for managing common Hastur messages.
  #
  module Message
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
    # receive a message from a ZeroMQ socket and return an appropriate Hastur::Message::* class,
    # e.g. route => :rawdata will return a Hastur::Message::Rawdata
    # 
    # object = Hastur::Message.recv(@socket)
    # object.route    # symbol for the route
    # object.envelope # Hastur::Envelope
    # object.payload  # usually JSON
    # object.send(@socket)
    #
    def self.recv(socket)
      messages = []
      rc = socket.recvmsgs messages

      payload = messages[-1].copy_out_string
      messages.pop.close

      envelope = Hastur::Envelope.parse messages[-1].copy_out_string
      messages.pop.close

      if klass = ROUTES[envelope.route]
        return klass.new(:payload => payload, :envelope => envelope)
      else
        raise ArgumentError.new "Invalid route '#{envelope.route}'"
      end
    end

    #
    # base class for the various Hastur::Message types
    #
    class Base
      attr_reader :envelope, :payload, :zmq_parts, :timestamp, :uptime, :sequence

      def initialize(envelope, payload, data, route, uuid)
        if envelope.nil? and uuid
          @envelope = Hastur::Envelope.new :route => route, :uuid => uuid
        elsif not envelope.nil?
          @envelope = envelope
        else
          raise ArgumentError.new "One of :envelope or :uuid arguments are required."
        end

        unless self.kind_of? ROUTES[@envelope.route]
          raise ArgumentError.new "Envelope route '#{@envelope.route.to_s}' does not match class '#{self.class}'"
        end

        if payload.nil? and not data.nil?
          unless data.respond_to?(:to_hash)
            raise ArgumentError.new "second argument must be nil or respond to to_hash()."
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

        if @zmq_parts and not @zmq_parts.empty?
          messages << @zmq_parts
          @zmq_parts.replace([]) # clear it
        end

        messages << ZMQ::Message.new(@envelope.pack)
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
        opts[:route] = :stat
        super(opts[:envelope], opts[:payload], opts[:stat], :stat, opts[:uuid])
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

        super(opts[:envelope], opts[:payload], data, :error, opts[:uuid])
      end
    end

    class Rawdata
      def initialize(opts)
        data = nil
        if opts[:rawdata]
          data = { :rawdata => opts[:rawdata] }
        end
        super(opts[:envelope], opts[:payload], data, :rawdata, opts[:uuid])
      end
    end

    class PluginExec
      def initialize(opts)
        super(opts[:envelope], opts[:payload], opts[:plugin_exec], :plugin_exec, opts[:uuid])
      end
    end

    class PluginResult
      def initialize(opts)
        super(opts[:envelope], opts[:payload], opts[:plugin_result], :plugin_result, opts[:uuid])
      end
    end

    class RegisterClient
      def initialize(opts)
        super(opts[:envelope], opts[:payload], opts[:register_client], :register_client, opts[:uuid])
      end
    end

    class RegisterService
      def initialize(opts)
        super(opts[:envelope], opts[:payload], opts[:register_service], :register_service, opts[:uuid])
      end
    end

    class RegisterPlugin
      def initialize(opts)
        super(opts[:envelope], opts[:payload], opts[:register_plugin], :register_plugin, opts[:uuid])
      end
    end
  end
end

