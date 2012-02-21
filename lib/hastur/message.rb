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
    if ROUTE_NAME.has_key? route or ROUTES.has_key? route.to_sym
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
      # make sure required arguments exist
      raise ArgumentError.new(":from is required") unless opts[:from]
      if opts[:to].nil? and opts.has_key? :route
        opts[:to] = Hastur.route_id(opts.delete :route)
      end
      raise ArgumentError.new(":to or :route is required") unless opts[:to]

      # make sure :to/:from are proper UUID's in 36-byte hex, but don't be
      # opinionated about them beyond that
      unless Hastur::Util.valid_uuid?(opts[:to])
        raise ArgumentError.new(":to => '#{opts[:to]}' is not a valid UUID")
      end
      unless Hastur::Util.valid_uuid?(opts[:from])
        raise ArgumentError.new(":from => '#{opts[:from]}' is not a valid UUID")
      end

      @version   = opts[:version] || VERSION
      @to        = opts[:to]
      @from      = opts[:from]
      @sequence  = opts[:sequence]  || Hastur::Util.next_seq
      @timestamp = opts[:timestamp] || (Time.new.to_f*1000000).to_i # convert to microseconds
      @uptime    = opts[:uptime]    || Hastur::Util.uptime(@timestamp)

      case opts[:ack]
        when true;   @ack = 1
        when false;  @ack = 0
        when Fixnum; @ack = opts[:ack]
        else;        @ack = 0
      end
    end

    #
    # check whether acks are enabled on the envelope
    #
    def ack?
      (@ack and @ack > 0) ? true : false
    end

    #
    # Return the envelope as a plain hash.
    #
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

    #
    # Return the envelope as a JSON string.
    #
    def to_json
      MultiJson.encode to_hash
    end

    #
    # Construct a Hastur::Envelope from a JSON string.
    #
    def self.from_json(json)
      data = MultiJson.decode json, :symbolize_keys => true
      self.new(data)
    end

    #
    # Return the envelope as a hex string representation of the on-wire data.
    #
    def to_s
      pack.unpack('H*')[0]
    end
  end

  #
  # A collection of classes for managing common Hastur messages.
  #
  module Message
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

      klass = ROUTE_KLASS[envelope.to] || ROUTE_KLASS[envelope.from]
      raise Hastur::UnsupportedError.new "no route in envelope: #{envelope.to_json}" unless klass
      klass.new :envelope => envelope, :payload => payload, :zmq_parts => messages
    end

    #
    # Like Envelope.parse but expects envelope + payload
    #
    # e.g.
    #  msg = Hastur::Message.parse(envelope, payload)
    #
    #  rc = socket.recvmsgs msgs=[]
    #  msg = Hastur::Message.parse(msgs[-2], msgs[-1])
    #
    # This is mostly intended to keep tests clean and does not do any error checking.
    #
    def self.parse(envelope_msg, payload_msg)
      envelope = Envelope.parse envelope_msg
      klass = route_class envelope.to
      klass.new :envelope => envelope, :payload => payload_msg
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
      def send(socket, opts={})
        raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
        opts[:final] ||= false

        if opts[:final]
          messages = @zmq_parts
        else
          messages = clone_zmq_parts
        end

        messages << ZMQ::Message.new(@envelope.pack)
        messages << ZMQ::Message.new(@payload.to_s)

        rc = socket.sendmsgs messages
        messages.each { |m| m.close }
        @zmq_parts = [] if opts[:final]
        rc
      end

      #
      # Close all of the related ZMQ::Message objects in msg.zmq_parts.
      #
      def close_zmq_parts
        @zmq_parts.each do |part|
          if part.kind_of? ZMQ::Message
            part.close
          else
            raise Hastur::BugError.new "an @zmq_part was not a ZMQ::Message. This is a fatal bug."
          end
        end
        @zmq_parts = []
      end

      #
      # Make a copy of all the ZMQ::Message parts in @zmq_parts, or empty list if there are none.
      # 
      def clone_zmq_parts
        @zmq_parts.map do |part|
          new = ZMQ::Message.new
          new.copy part.pointer
          new
        end
      end

      #
      # convert the message to a hash suitable for serialization
      #
      def to_hash
        {
          :klass     => self.class,
          :envelope  => @envelope.to_hash,
          :zmq_parts => @zmq_parts,
          :payload   => @payload,
        }
      end

      #
      # return the message as a string of json
      # zmq_parts will be encoded in hex
      #
      def to_json
        hash = to_hash
        hash[:zmq_parts] = hash.delete(:zmq_parts).map { |p| p.copy_out_string.unpack('H*')[0] }
        MultiJson.encode hash
      end

      #
      # decode a json data structure into an object
      #
      def self.from_json(json)
        hash = MultiJson.decode json, :symbolize_keys => true
        hash[:zmq_parts] = hash.delete(:zmq_parts).map { |p| ZMQ::Message.new([p].pack('H*')) }
        hash[:envelope] = Envelope.new hash.delete(:envelope)
        self.new(hash)
      end

      #
      # returns the payload as-is
      #
      def to_s
        payload
      end

      #
      # returns whether this class's payload is usually json or not
      #
      def self.json?
        true
      end

      #
      # Return a data structure rather than raw JSON.
      #
      def decode
        MultiJson.decode @payload, :symbolize_keys => true
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
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:stat]
        super(opts)
      end
    end

    #
    # m = Hastur::Message::Log.new :from => from, :payload => string
    #
    class Log
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:log]
        super(opts)
      end

      def self.json?
        false
      end
    end

    #
    # a notification, acks are enabled by default
    #
    class Notification
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:notification]
        opts[:ack] = true unless opts.has_key?(:ack)
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
        return super(opts) if opts.has_key? :envelope

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

      def self.json?
        false
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
        return super(opts_in) if opts_in.has_key? :envelope

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
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:rawdata]
        opts[:payload] = opts.delete :payload
        raise ArgumentError.new "Rawdata only supports raw payloads, e.g. :payload => 'stuff'" if opts[:data]
        super(opts)
      end

      def self.json?
        false
      end
    end

    class HeartbeatClient
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:heartbeat_client]
        super(opts)
      end
    end

    # TODO: what do we want in these?
    class HeartbeatService
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:heartbeat_service]
        super(opts)
      end
    end

    class PluginExec
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope

        opts[:from] = ROUTES[:plugin_exec]

        unless Hastur::Util.valid_uuid?(opts[:to])
          raise ArgumentError.new("'#{opts[:to]}' is not a valid UUID")
        end

        super(opts)
      end

      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end
    end

    class PluginResult
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:plugin_result]
        super(opts)
      end
    end

    class RegisterClient
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:register_client]
        opts[:data] = {
          :_route => 'register_client',
          :uuid     => opts[:from],
          :hostname => Socket.gethostname,
          :ipv4     => IPSocket.getaddress(Socket.gethostname),
        }

        super(opts)
      end
    end

    class RegisterService
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:register_service]
        super(opts)
      end
    end

    class RegisterPlugin
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:register_plugin]
        super(opts)
      end
    end
  end
end

