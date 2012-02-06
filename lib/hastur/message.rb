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
    class Base;                   end
    class Request         < Base; end
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
    :request          => Hastur::Message::Request,
    :rawdata          => Hastur::Message::Rawdata,
    :plugin_exec      => Hastur::Message::PluginExec,
    :plugin_result    => Hastur::Message::PluginResult,
    :register_client  => Hastur::Message::RegisterClient,
    :register_plugin  => Hastur::Message::RegisterPlugin,
    :register_service => Hastur::Message::RegisterService,
  }

  UUID_RE = /\A[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z/i

  #
  # keep a single, global counter for the :sequence field
  #
  @counter = 0
  def self.next
    @counter+=1
  end

  #
  # parsing & creating Hastur envelopes, V1
  # Format:
  # field: version route      uuid         ack    sequence timestamp uptime
  # type:  <int16> <char[16]> <int128>     <int8> <int64>  <double>  <double>
  # pack:  n       Z16        H8H4H4H4H12  C      Q>       G         G
  #
  # Version doesn't really have to be bumped unless one of these fields
  # changes type incompatibly.  For example, we can add an HMAC field on the
  # end later on and old unpacks will just ignore data at the end.
  #
  # Numbers are big endian wherever it makes sense.
  # 
  class Envelope
    VERSION = 1
    PACK =  %w[ n         Z16     H8H4H4H4H12 C     Q>         G           G ].join
    #           0         1       2-6         7     8          9           10
    attr_reader :version, :route, :uuid,      :ack, :sequence, :timestamp, :uptime

    #
    # parse a Hastur routing envelope, usually in the multi part just ahead of the payload
    # it is a binary string with a version, the route (string), an ack flag (1/0), and the UUID
    #
    def self.parse(msg)
      parts = msg.unpack(PACK)
      self.new(
        :version   => parts[0],
        :route     => parts[1].to_sym,
        :uuid      => parts[2..6].join('-'),
        :ack       => parts[7],
        :sequence  => parts[8],
        :timestamp => parts[9],
        :uptime    => parts[10],
      )
    end

    #
    # pack an envelope into a binary string, ready to send on the wire
    #
    def pack
      [@version, @route.to_s, *@uuid.split(/-/), @ack, @sequence, @timestamp, @uptime].pack(PACK)
    end

    #
    # create a new envelope, only the route is required and acks can be enabled (default off)
    #
    def initialize(opts)
      raise ArgumentError.new(":route is required") unless opts[:route]
      raise ArgumentError.new(":uuid is required") unless opts[:uuid]
      raise ArgumentError.new("Invalid route '#{opts[:route]}'") unless ROUTES.has_key?(opts[:route].to_sym)
      raise ArgumentError.new("'#{opts[:uuid]} is not a valid UUID") unless UUID_RE.match(opts[:uuid])

      @version   = opts[:version] || VERSION
      @route     = opts[:route].to_sym
      @uuid      = opts[:uuid]
      @sequence  = opts[:sequence]  || Hastur.next
      @timestamp = opts[:timestamp] || Time.new.to_f
      @uptime    = opts[:uptime]    || @timestamp - BOOT_TIME

      case opts[:ack]
        when true; @ack = 1
        when 1;    @ack = 1
        else;      @ack = 0
      end
    end
    
    #
    # check whether acks are enabled on the envelope
    #
    def ack?
      @ack == 0 ? false : true
    end

    def to_s
      pack.unpack('H*')[0]
    end
  end

  #
  # A collection of classes for managing common Hastur messages.
  #
  module Message
    def self.create(opts)
      envelope = opts[:envelope] or raise ArgumentError.new(":envelope is required")
      payload  = opts[:payload]  or raise ArgumentError.new(":payload is required")
      
      if klass = ROUTES[envelope.route]
        return klass.new(:payload => payload, :envelope => envelope)
      else
        raise ArgumentError.new "Invalid route in envelope: '#{envelope.route}'"
      end
    end

    #
    # base class for the various Hastur::Message types
    #
    class Base
      attr_reader :envelope, :payload
      attr_accessor :zmq_parts

      # this should only really ever get called via super() in the
      # subclasses below
      def initialize(opts)
        if opts[:envelope].kind_of? Hastur::Envelope
          @envelope = opts[:envelope]
        # automatically construct an envelope if :uuid & :route are provided (all flags passed through)
        elsif not opts[:envelope] and opts[:uuid] and opts[:route]
          @envelope = Hastur::Envelope.new opts
        else
          raise ArgumentError.new ":envelope or :uuid/:route arguments are required."
        end

        unless self.kind_of? ROUTES[@envelope.route]
          raise ArgumentError.new "Envelope route '#{@envelope.route.to_s}' does not match class '#{self.class}'"
        end

        if opts[:data].respond_to? :to_hash and not opts[:payload]
          @payload = MultiJson.encode opts[:data].to_hash
        elsif opts[:payload]
          @payload = opts[:payload]
        else
          raise ArgumentError.new "Either :data or :payload is required."
        end

        if opts[:zmq_parts] and opts[:zmq_parts].length > 0
          @zmq_parts = opts[:zmq_parts]
        end
      end

      def self.request(socket, opts={})
        raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
        opts[:uuid] ||= SecureRandom.uuid

        e = Hastur::Envelope.new :uuid => opts[:uuid], :route => :request

        messages = [ ZMQ::Message.new(e.pack), ZMQ::Message.new('') ]

        rc = socket.sendmsgs messages
        messages.each { |m| m.close }
        rc
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
      def self.recv(socket, zmq_flags=0)
        raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
        messages = []
        rc = socket.recvmsgs messages, zmq_flags
        return rc if zmq_flags != 0 and rc == -1

        raise "socket.recvmsgs failed" unless rc != -1

        payload = messages[-1].copy_out_string
        messages.pop.close

        envelope = Hastur::Envelope.parse messages[-1].copy_out_string
        messages.pop.close

        self.new :envelope => envelope, :payload => payload, :zmq_parts => messages
      end

      #
      # send the message on a ZeroMQ socket. This is not particular about what kind of ZeroMQ socket.
      # Care is taken to try to use ZMQ::Message as-is without converting to/from strings until it's
      # necessary. Generally this only helps with router envelopes where they exist, since ZMQ::Message's
      # are generally use-once only.
      #
      def send(socket)
        raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
        messages = []

        unless @zmq_parts.nil? or @zmq_parts.empty?
          messages = @zmq_parts.clone
          @zmq_parts.clear
        end

        messages << ZMQ::Message.new(@envelope.pack)
        messages << ZMQ::Message.new(@payload.to_s)

        rc = socket.sendmsgs messages
        messages.each { |m| m.close }
        rc
      end

      def to_s
        payload
      end
    end

    class Request
      def initialize(socket)
        raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
        STDERR.write '-'
        @socket = socket
      end

      def reply(message)
        message.send(@socket)
        STDERR.write '+'
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
        opts[:data]  = opts.delete :stat
        super(opts)
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
        if opts.kind_of? String
          opts = {
            :route   => :error,
            :payload => { :error => opts }
          }
        elsif opts[:error]
          opts[:route]   = :error
          opts[:payload] = MultiJson.encode opts.delete :error
        end

        super(opts)
      end
    end

    class Rawdata
      def initialize(opts)
        opts[:route]   = :rawdata
        opts[:payload] = opts.delete :payload
        super(opts)
      end
    end

    class PluginExec
      def initialize(opts)
        opts[:route] = :plugin_exec
        super(opts)
      end
    end

    class PluginResult
      def initialize(opts)
        opts[:route] = :plugin_result
        super(opts)
      end
    end

    class RegisterClient
      def initialize(opts)
        opts[:route] = :register_client
        super(opts)
      end
    end

    class RegisterService
      def initialize(opts)
        opts[:route] = :register_service
        super(opts)
      end
    end

    class RegisterPlugin
      def initialize(opts)
        opts[:route] = :register_plugin
        super(opts)
      end
    end
  end
end

