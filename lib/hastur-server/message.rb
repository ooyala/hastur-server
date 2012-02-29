require 'ffi-rzmq'
require 'multi_json'
require 'hastur-server/exception'
require 'hastur-server/util'
require 'openssl'
require 'base64'

module Hastur
  #
  # Handle all of the busywork around Hastur messages consistently.
  #
  # Updating existing messages is not currently supported. Unless we come across a clear case where
  # it will help clean up code or improve performance measurably, it won't happen.
  #
  module Message
    # pre-declare the class structure to make introspection work
    class Base;                end
    class Rawdata      < Base; end
    class Ack          < Base; end
    class Stat         < Base; end
    class Event        < Base; end
    class Log          < Base; end
    class Error        < Base; end
    class Heartbeat    < Base; end
    class PluginExec   < Base; end
    class Registration < Base; end
  end

  # map human-readable route names to their 128-bit "UUID", in this case, it's not actually a GUID
  # but instead the strings encoded with:
  # ["string"].pack('Z16').unpack('H8H4H4H4H12').join('-')
  # Acks are strictly point-to-point, so they don't make an appearance here.
  ROUTES = {
    :stat         => '73746174-0000-0000-0000-000000000000',
    :event        => '6576656e-7400-0000-0000-000000000000',
    :log          => '6c6f6700-0000-0000-0000-000000000000',
    :error        => '6572726f-7200-0000-0000-000000000000',
    :rawdata      => '72617764-6174-6100-0000-000000000000',
    :heartbeat    => '68656172-7462-6561-7400-000000000000',
    :registration => '72656769-7374-7261-7469-6f6e00000000',
  }

  ROUTE_NAME = ROUTES.invert

  # easy mapping of route id's to handler classes
  ROUTE_KLASS = {
    '73746174-0000-0000-0000-000000000000' => Hastur::Message::Stat,
    '6576656e-7400-0000-0000-000000000000' => Hastur::Message::Event,
    '6c6f6700-0000-0000-0000-000000000000' => Hastur::Message::Log,
    '6572726f-7200-0000-0000-000000000000' => Hastur::Message::Error,
    '72617764-6174-6100-0000-000000000000' => Hastur::Message::Rawdata,
    '68656172-7462-6561-7400-000000000000' => Hastur::Message::Heartbeat,
    '72656769-7374-7261-7469-6f6e00000000' => Hastur::Message::Registration,
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
  # field: version to           from         ack    sequence timestamp uptime   hmac     router
  # type:  <int16> <int128>     <int128>     <int8> <int64>  <int64>   <int64>  <int256> <int128> ....
  # pack:  n       H8H4H4H4H12  H8H4H4H4H12  C      Q>       Q>        Q>       H64      H8H4H4H4H12
  #
  # Version doesn't really have to be bumped unless one of these fields
  # changes type incompatibly.  For example, we can add an HMAC field on the
  # end later on and old unpacks will just ignore data at the end.
  #
  # Numbers are big endian wherever it makes sense.
  # 
  class Envelope
    VERSION = 1
    DIGEST = OpenSSL::Digest::Digest.new('sha256')

    # pass the envelope around as a binary packed string - the routers should be able to parse this
    # quickly without diving into JSON or anything not built directly into the language
    PACK =  %w[ n         H8H4H4H4H12 H8H4H4H4H12 C     C        Q>         Q>          Q>       H64    a* ].join
    #           0         1-5         6-10        11    12       13         14          15       16     17
    attr_reader :version, :to,        :from,      :ack, :resend, :sequence, :timestamp, :uptime, :hmac, :routers

    ROUTER_OFFSET = 1

    #
    # parse a Hastur routing envelope, usually in the multi part just ahead of the payload
    #
    def self.parse(msg)
      parts = msg.unpack(PACK)

      # the router can append its UUID to the end of the envelope before sending it on so we have
      # traceroute-like functionality (and debug-ability)
      routers = []
      if not parts[17].empty? and parts[17].length % 16 == 0
        0.upto(parts[17].length / 16) do |i|
          routers << parts[17].unpack("@#{i}H8H4H4H4H12").join('-')
        end
      end

      self.new(
        :version   => parts[0],
        :to        => parts[1..5].join('-'),
        :from      => parts[6..10].join('-'),
        :ack       => parts[11],
        :resend    => parts[12],
        :sequence  => parts[13],
        :timestamp => parts[14],
        :uptime    => parts[15],
        :hmac      => parts[16],
        :routers   => routers
      )
    end

    #
    # pack an envelope into a binary string, ready to send on the wire
    #
    def pack
      routers = ''
      if @routers.any?
        routers = @routers.map { |r| r.split('-').pack('H8H4H4H4H12') }.join('')
      end

      [
        @version,
        @to.split(/-/),
        @from.split(/-/),
        @ack,
        @resend,
        @sequence,
        @timestamp,
        @uptime,
        @hmac,
        routers,
      ].flatten.pack(PACK)
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

      @version   = opts[:version]  || VERSION
      @to        = opts[:to]
      @from      = opts[:from]
      @resend    = opts[:resend]   || 0
      @sequence  = opts[:sequence] || Hastur::Util.next_seq
      @timestamp = Hastur::Util.timestamp(opts[:timestamp])
      @uptime    = opts[:uptime]   || Hastur::Util.uptime(@timestamp)
      @hmac      = opts[:hmac]     || ''
      @routers   = opts[:routers]  || []

      case opts[:ack]
        when true;   @ack = 1
        when false;  @ack = 0
        when Fixnum; @ack = opts[:ack]
        else;        @ack = 0
      end
    end

    #
    # update the hmac field using the provided secret/data
    #
    def update_hmac(secret, data)
      hmac = OpenSSL::HMAC.digest(DIGEST, secret, data)
      @hmac = hmac.unpack('H64')[0]
    end

    #
    # append a router's uuid to the envelope's routing history
    #
    def add_router(router)
      @routers << router
    end

    #
    # increment resend counter
    #
    def incr_resend
      @resend += 1
    end

    #
    # check whether acks are enabled on the envelope
    #
    def ack?
      (@ack and @ack > 0) ? true : false
    end

    def to_ack(from=@envelope.to)
      Hastur::Message::Ack.new(
        :to   => @envelope.from,
        :from => from,
        :data => @envelope
      )
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
        raise ArgumentError.new "Only hash arguments are supported." unless opts.kind_of? Hash

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

        @secret = opts[:secret] || ''

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

        @envelope.update_hmac(@payload, opts[:secret] || @secret)

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
      # Decode the JSON payload. This may be overridden in subclasses of Base.
      # Does not validate.
      #
      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end

      #
      # Set the payload to the serialized JSON of the given data structure.
      # Does not validate.
      #
      def encode(data)
        @payload = MultiJson.encode data
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
    # a general event
    #
    class Event
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:event]
        opts[:ack] = true
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
    end

    #
    # generally the ack_id will the the ack'ed message's envelope
    # e.g. ack = Hastur::Message::Ack.new :from => uuid, :data => msg.envelope
    # the destination UUID is automatically extracted from the envelope
    #
    # Or, just call msg.to_ack.send(socket). (implemented in Hastur::Message::Base)
    #
    class Ack
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope

        unless opts[:data].kind_of? Hastur::Envelope
          raise ArgumentError.new "acks can only be created from Hastur::Envelope objects"
        end

        opts[:to]      = opts[:from] || opts[:data].from
        opts[:from]    = opts[:to]   || opts[:data].to
        opts[:payload] = opts[:data].to_json
        opts[:ack]     = false

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
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:rawdata]

        if opts[:data]
          opts[:payload] = encode(opts.delete(:data))
        end

        super(opts)
      end

      #
      # Always base64 encode any data in an Error, because it may be malformed
      # or even (accidentally) malicious data.  The transmitted JSON should always
      # have two keys, :error and :data, where the :data value is always base64
      # encoded.
      # 
      # A couple automatic conversions are executed:
      #   Hastur::Message::Base -> to_json -> base64
      #   Exception -> .inspect -> base64
      #   Array / Hash -> JSON encoded -> base64
      #   String -> base64
      #   * -> .inspect -> base64
      #
      def encode(data)
        case data
          when Hastur::Message::Base
            error = :message
            data = opts.delete(:data).to_json
          when Exception
            error = :exception
            data = opts.delete(:data).inspect
          when Hash
          when Array
            error = :structured
            data = MultiJson.encode(opts.delete(:data))
          when String
            error = :raw
            data = opts.delete(:data)
          else
            error = :undefined
            data = opts.delete(:data).inspect
        end

        @payload = MultiJson.encode({
          :error => error,
          :data  => Base64.encode(data)
        })
      end

      #
      # Convert the JSON payload described above into a hash and decode the Mime64
      # part. Does not decode any further, so data structures, etc. that were encoded
      # by e.encode (e.g. hash -> JSON), are left in whatever that encoding is.
      #
      def decode
        hash = super
        hash[:data] = Base64.decode(hash[:data])
        hash
      end
    end

    class Rawdata
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:rawdata]
        opts[:payload] = Base64.encode(opts.delete(:payload))
        raise ArgumentError.new "Rawdata only supports raw payloads, e.g. :payload => 'stuff'" if opts[:data]
        super(opts)
      end

      #
      # Update payload value with the base64 encoding of the provided string.
      #
      def encode(data)
        @payload = Base64.encode(data)
      end

      #
      # Decode the payload from base64 to a string.
      #
      def decode
        Base64.decode(@payload)
      end
    end

    class Heartbeat
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:heartbeat]
        super(opts)
      end
    end

    class PluginExec
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope

        unless Hastur::Util.valid_uuid?(opts[:to])
          raise ArgumentError.new("'to' field, '#{opts[:to]}', is not a valid UUID")
        end

        unless Hastur::Util.valid_uuid?(opts[:from])
          raise ArgumentError.new("'from' field, '#{opts[:from]}', is not a valid UUID")
        end

        super(opts)
      end

      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end
    end

    class Registration
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = ROUTES[:registration]
        super(opts)
      end
    end
  end
end

