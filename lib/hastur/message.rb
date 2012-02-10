require 'ffi-rzmq'
require 'multi_json'
require 'hastur/exception'
require 'hastur/util'

module Hastur
  #
  # Handle all of the busywork around Hastur messages consistently.
  #
  # Updating existing messages is not currently supported. Unless we come across a clear case where
  # it will help clean up code or improve performance measurably, it won't happen.
  #
  module Message
    # pre-declare the class structure to make introspection work
    class Base;                    end
    class Stat             < Base; end
    class Log              < Base; end
    class Error            < Base; end
    class Rawdata          < Base; end
    class Notification     < Base; end
    class HeartbeatClient  < Base; end
    class HeartbeatService < Base; end
    class PluginExec       < Base; end
    class PluginResult     < Base; end
    class RegisterClient   < Base; end
    class RegisterPlugin   < Base; end
    class RegisterService  < Base; end
  end

  # map human-readable route names to their 128-bit "UUID", in this case, it's not actually a GUID
  # but instead the strings encoded with:
  # ["string"].pack('Z16').unpack('H8H4H4H4H12').join('-')
  ROUTES = {
    :stat              => '73746174-0000-0000-0000-000000000000',
    :log               => '6c6f6700-0000-0000-0000-000000000000',
    :error             => '6572726f-7200-0000-0000-000000000000',
    :rawdata           => '72617764-6174-6100-0000-000000000000',
    :notification      => '6e6f7469-6669-6361-7469-6f6e00000000',
    :heartbeat_client  => '68656172-7462-6561-745f-636c69656e74',
    :heartbeat_service => '68656172-7462-6561-745f-736572766963',
    :plugin_exec       => '706c7567-696e-5f65-7865-630000000000',
    :plugin_result     => '706c7567-696e-5f72-6573-756c74000000',
    :register_client   => '72656769-7374-6572-5f63-6c69656e7400',
    :register_plugin   => '72656769-7374-6572-5f70-6c7567696e00',
    :register_service  => '72656769-7374-6572-5f73-657276696365',
  }

  ROUTE_NAME = ROUTES.invert

  # easy mapping of route id's to handler classes
  ROUTE_KLASS = {
    '73746174-0000-0000-0000-000000000000' => Hastur::Message::Stat,
    '6c6f6700-0000-0000-0000-000000000000' => Hastur::Message::Log,
    '6572726f-7200-0000-0000-000000000000' => Hastur::Message::Error,
    '72617764-6174-6100-0000-000000000000' => Hastur::Message::Rawdata,
    '6e6f7469-6669-6361-7469-6f6e00000000' => Hastur::Message::Notification,
    '68656172-7462-6561-745f-636c69656e74' => Hastur::Message::HeartbeatClient,
    '68656172-7462-6561-745f-736572766963' => Hastur::Message::HeartbeatService,
    '706c7567-696e-5f65-7865-630000000000' => Hastur::Message::PluginExec,
    '706c7567-696e-5f72-6573-756c74000000' => Hastur::Message::PluginResult,
    '72656769-7374-6572-5f63-6c69656e7400' => Hastur::Message::RegisterClient,
    '72656769-7374-6572-5f70-6c7567696e00' => Hastur::Message::RegisterPlugin,
    '72656769-7374-6572-5f73-657276696365' => Hastur::Message::RegisterService,
  }

  #
  # Given either a route UUID or symbol, always return the UUID.
  # Raises an argument exception if the provided value is not a valid route symbol/uuid.
  #
  def self.route_id(route)
    if ROUTE_NAME.has_key? route
      route
    elsif ROUTES.has_key? route.to_sym
      ROUTES[route.to_sym]
    else
      raise ArgumentError.new "'#{route}' is not a valid route symbol or uuid"
    end
  end

  #
  # Given either a route UUID or symbol, always return the symbol.
  # Raises an argument exception if the provided value is not a valid route symbol/uuid.
  #
  def self.route_symbol(route)
    if ROUTE_NAME.has_key? route
      ROUTE_NAME[route]
    elsif ROUTES.has_key? route.to_sym
      route.to_sym
    else
      raise ArgumentError.new "'#{route}' is not a valid route symbol or uuid"
    end
  end

  #
  # Given either a route UUID or symbol, return true/false if it's valid for routing
  #
  def self.route?(route)
    if ROUTE_NAME.has_key? route
      true
    elsif ROUTES.has_key? route.to_sym
      true
    else
      false
    end
  end

  # parsing & creating Hastur envelopes, V1
  # Format:
  # field: version to           from         ack    sequence timestamp uptime
  # type:  <int16> <int128>     <int128>     <int8> <int64>  <double>  <double>
  # pack:  n       H8H4H4H4H12  H8H4H4H4H12  C      Q>       G         G
  #
  # Version doesn't really have to be bumped unless one of these fields
  # changes type incompatibly.  For example, we can add an HMAC field on the
  # end later on and old unpacks will just ignore data at the end.
  #
  # Numbers are big endian wherever it makes sense.
  # 
  class Envelope
    VERSION = 1
    PACK =  %w[ n         H8H4H4H4H12 H8H4H4H4H12 C     Q>         G           G ].join
    #           0         1-5         6-10        11    12         13          14
    attr_reader :version, :to,        :from,      :ack, :sequence, :timestamp, :uptime

    #
    # parse a Hastur routing envelope, usually in the multi part just ahead of the payload
    # it is a binary string with a version, the route (string), an ack flag (1/0), and the UUID
    #
    def self.parse(msg)
      parts = msg.unpack(PACK)
      self.new(
        :version   => parts[0],
        :to        => parts[1..5].join('-'),
        :from      => parts[6..10].join('-'),
        :ack       => parts[11],
        :sequence  => parts[12],
        :timestamp => parts[13],
        :uptime    => parts[14],
      )
    end

    #
    # pack an envelope into a binary string, ready to send on the wire
    #
    def pack
      [@version, *@to.split(/-/), *@from.split(/-/), @ack, @sequence, @timestamp, @uptime].pack(PACK)
    end

    #
    # create a new envelope
    # :to is required, but can be passed as :route => SYM to be human readable
    # :from is required, generally the client UUID
    # :ack is optional, defaults to disabled
    # :sequence, :timestamp, and :uptime are optional and will be set to sane defaults
    #
    def initialize(opts)
      raise ArgumentError.new(":from is required") unless opts[:from]
      raise ArgumentError.new("'#{opts[:from]}' is not a valid UUID") unless Hastur::Util.valid_uuid?(opts[:from])

      if opts[:to].nil? and opts.has_key? :route
        opts[:to] = Hastur.route_id(opts.delete :route)
      end

      raise ArgumentError.new(":to or :route is required") unless opts[:to]

      if opts[:reversed] 
        unless Hastur::Util.valid_uuid?(opts[:to])
          raise ArgumentError.new("'#{opts[:to]}' is not a valid UUID")
        end
      else
        unless Hastur.route?(opts[:to])
          raise ArgumentError.new(":to '#{opts[:to]}' is not a valid route") 
        end
      end

      @version   = opts[:version] || VERSION
      @to        = opts[:to]
      @from      = opts[:from]
      @sequence  = opts[:sequence]  || Hastur::Util.next_seq
      @timestamp = opts[:timestamp] || Time.new.to_f
      @uptime    = opts[:uptime]    || Hastur::Util.uptime(@timestamp)

      case opts[:ack]
        when true;   @ack = 1
        when false;  @ack = 0
        when Fixnum; @ack = opts[:ack]
        else;        @ack = 0
      end
    end

    def route
      ROUTE_NAME[@to]
    end
    
    #
    # check whether acks are enabled on the envelope
    #
    def ack?
      @ack == 0 ? false : true
    end

    def to_hash
      {
        :version   => @version,
        :to        => @to,
        :from      => @from,
        :ack       => @ack,
        :sequence  => @sequence,
        :timestamp => @timestamp,
        :uptime    => @uptime,
      }
    end

    def to_json
      MultiJson.encode to_hash
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

      if klass = ROUTE_KLASS[envelope.to]
        return klass.new(:payload => payload, :envelope => envelope)
      else
        raise ArgumentError.new "Invalid route in envelope: '#{envelope.route}'"
      end
    end

    # return the class for a given route string/symbol
    # e.g. klass = Hastur::Message.route_class("notification")
    def self.route_class(route)
      route_id = Hastur.route_id(route)
      ROUTE_KLASS[route_id]
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

      if ROUTE_KLASS.has_key? envelope.to
        klass = ROUTE_KLASS[envelope.to]
        klass.new :envelope => envelope, :payload => payload, :zmq_parts => messages
      else
        raise Hastur::UnsupportedError.new "unsupported route in envelope: #{envelope.to_json}"
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
        # automatically construct an envelope if :from & :route are provided (all flags passed through)
        elsif not opts[:envelope] and opts[:from] and (opts[:route] or opts[:to])
          @envelope = Hastur::Envelope.new opts
        else
          raise ArgumentError.new ":envelope or :from/:route arguments are required."
        end

        if opts[:data].respond_to? :to_hash and not opts[:payload]
          @payload = MultiJson.encode opts[:data].to_hash
        elsif opts[:payload]
          @payload = opts[:payload]
        else
          raise ArgumentError.new "Exactly one of :data or :payload is required."
        end

        if opts[:zmq_parts] and opts[:zmq_parts].length > 0
          @zmq_parts = opts[:zmq_parts]
        else
          @zmq_parts = []
        end
      end

      #
      # send the message on a ZeroMQ socket. This is not particular about what kind of ZeroMQ socket.
      # Care is taken to try to use ZMQ::Message as-is without converting to/from strings until it's
      # necessary. Generally this only helps with router envelopes where they exist, since ZMQ::Message's
      # are generally use-once only.
      #
      # Messages can be sent more than once.
      #
      # Messages with zmq_parts will not automatically close the ZMQ::Message objects. Call msg.close.
      #
      def send(socket)
        raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
        messages = []

        @zmq_parts.each do |part|
          if part.kind_of? ZMQ::Message
            # copy zmq parts rather than using them in case a message needs 
            messages << ZMQ::Message.new
            messages[-1].copy part.pointer
          else
            raise Hastur::BugError.new "an @zmq_part was not a ZMQ::Message. This is a fatal bug."
          end
        end

        messages << ZMQ::Message.new(@envelope.pack)
        messages << ZMQ::Message.new(@payload.to_s)

        rc = socket.sendmsgs messages
        messages.each { |m| m.close }
        rc
      end

      #
      # Close all of the related ZMQ::Message objects in msg.zmq_parts.
      #
      def close
        @zmq_parts.each do |part|
          if part.kind_of? ZMQ::Message
            part.close
          else
            raise Hastur::BugError.new "an @zmq_part was not a ZMQ::Message. This is a fatal bug."
          end
        end
      end

      def to_hash
        {
          :klass     => self.class,
          :envelope  => @envelope.to_hash,
          :zmq_parts => @zmq_parts,
          :payload   => @payload,
        }
      end

      def to_json
        MultiJson.encode to_hash
      end

      def to_s
        payload
      end
    end

    #
    # s = Hastur::Message::Stat.new(:from => from, :payload => json_string)
    #
    # stat = Hastur::Stat.new( ... )
    # s = Hastur::Message::Stat.new(stat)
    # 
    class Stat
      def initialize(opts)
        opts[:to] = ROUTES[:stat]
        super(opts)
      end

      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end
    end

    #
    # m = Hastur::Message::Log.new :from => from, :payload => string
    #
    class Log
      def initialize(opts)
        opts[:to] = ROUTES[:log]
        super(opts)
      end
    end

    #
    # a notification, acks are enabled by default
    #
    class Notification
      def initialize(opts)
        opts[:to] = ROUTES[:log]
        unless opts.has_key? :ack
          opts[:ack] = true
        end
        super(opts)
      end
    end

    #
    # an ack
    # generally the ack_id will the the ack'ed message's envelope
    # e.g. ack = Hastur::Message::Ack.new :from => uuid, :data => msg.envelope
    # the destination UUID is automatically extracted from the envelope
    #
    class Ack
      def initialize(opts)
        unless opts[:data].kind_of? Hastur::Envelope
          raise ArgumentError.new "acks can only be created from Hastur::Envelope objects"
        end

        opts[:to] = opts[:data].from
        opts[:payload] = opts[:data].pack
        opts[:ack] = false
        opts[:reversed] = true # data flows from core -> client

        super(opts)
      end

      #
      # Return the envelope of the acked message.
      #
      def acked
        Hastur::Envelope.parse @payload
      end
    end

    #
    # When given a straight payload, it's passed through unmodified. Otherwise,
    # it'll try to DTRT for most inputs, even outside of hash paremeters.
    #
    # s = Hastur::Message::Error.new :payload => json_string
    #
    # rescue FooBar => e
    #   s = Hastur::Message::Error.new e
    # end
    # 
    class Error
      def initialize(opts_in)
        opts = { :to => ROUTES[:error] }

        if opts_in.kind_of? Hastur::Message::Base
          opts[:from] = opts_in.envelope.from
          opts[:payload] = MultiJson.encode(opts_in.to_hash)
        elsif opts_in.kind_of? Hash
          opts.merge! opts_in
        end

        super(opts)
      end
    end

    class Rawdata
      def initialize(opts)
        opts[:to] = ROUTES[:rawdata]
        opts[:payload] = opts.delete :payload
        raise ArgumentError.new "Rawdata only supports raw payloads, e.g. :payload => 'stuff'" if opts[:data]
        super(opts)
      end
    end

    class HeartbeatClient
      def initialize(opts)
        opts[:to] = ROUTES[:heartbeat_client]
        opts[:payload] = ''
        super(opts)
      end
    end

    # TODO: what do we want in these?
    class HeartbeatService
      def initialize(opts)
        opts[:to] = ROUTES[:heartbeat_service]
        super(opts)
      end
    end

    class PluginExec
      def initialize(opts)
        unless Hastur::Util.valid_uuid?(opts[:to])
          raise ArgumentError.new("'#{opts[:to]}' is not a valid UUID")
        end

        opts[:reversed] = true # data flows from core -> client

        super(opts)
      end

      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end
    end

    class PluginResult
      def initialize(opts)
        opts[:to] = ROUTES[:plugin_result]
        super(opts)
      end
    end

    class RegisterClient
      def initialize(opts)
        opts[:to] = ROUTES[:register_client]
        opts[:data] = {
          :method => 'register_client',
          :params => {
            :uuid     => opts[:from],
            :hostname => Socket.gethostname,
            :ipv4     => IPSocket.getaddress(Socket.gethostname),
          }
        }

        super(opts)
      end
    end

    class RegisterService
      def initialize(opts)
        opts[:to] = ROUTES[:register_service]
        super(opts)
      end
    end

    class RegisterPlugin
      def initialize(opts)
        opts[:to] = ROUTES[:register_plugin]
        super(opts)
      end
    end
  end
end

