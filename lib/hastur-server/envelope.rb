require 'ffi-rzmq'
require 'multi_json'
require 'hastur-server/util'
require 'openssl'

module Hastur
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
    PACK =  %w[ n C H8H4H4H4H12 H8H4H4H4H12 C C Q> Q> Q> H64 a* ].join
    attr_reader :version, :type_id, :to, :from, :ack, :resend, :sequence, :timestamp, :uptime, :hmac, :routers

    VERSION_IDX     = 0
    TYPE_IDX        = 1
    TO_UUID_IDX     = 2..6
    FROM_UUID_IDX   = 7..11
    ACK_IDX         = 12
    RESEND_IDX      = 13
    SEQUENCE_IDX    = 14
    TIMESTAMP_IDX   = 15
    UPTIME_IDX      = 16
    HMAC_IDX        = 17
    ROUTER_LIST_IDX = 18

    #
    # parse a Hastur routing envelope, usually in the multi part just ahead of the payload
    #
    def self.parse(msg)
      parts = msg.unpack(PACK)

      # the router can append its UUID to the end of the envelope before sending it on so we have
      # traceroute-like functionality (and debug-ability)
      routers = []
      if not parts[ROUTER_LIST_IDX].empty? and parts[ROUTER_LIST_IDX].length % 16 == 0
        0.step(parts[ROUTER_LIST_IDX].length, 16) do |position|
          routers << parts[ROUTER_LIST_IDX].unpack("@#{position}H8H4H4H4H12").join('-')
        end
      end

      self.new(
        :version   => parts[VERSION_IDX],
        :type_id   => parts[TYPE_IDX],
        :to        => parts[TO_UUID_IDX].join('-'),
        :from      => parts[FROM_UUID_IDX].join('-'),
        :ack       => parts[ACK_IDX],
        :resend    => parts[RESEND_IDX],
        :sequence  => parts[SEQUENCE_IDX],
        :timestamp => parts[TIMESTAMP_IDX],
        :uptime    => parts[UPTIME_IDX],
        :hmac      => parts[HMAC_IDX],
        :routers   => routers.select { |r| r.length == 36 }
      )
    end

    #
    # pack an envelope into a binary string, ready to send on the wire
    #
    def pack
      routers = ''
      if @routers.any?
        routers = @routers.select { |r| r.length == 36 }.map { |r| r.split('-').pack('H8H4H4H4H12') }.join('')
      end

      [
        @version,
        @type_id,
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
    # :to is required, usually Hastur::Message::*::ROUTE_UUID
    # :from is required, generally the agent UUID
    # :ack is optional, defaults to disabled
    # :sequence, :timestamp, and :uptime are optional and will be set to sane defaults
    #
    def initialize(opts)
      # make sure required arguments exist
      raise ArgumentError.new(":from is required") unless opts[:from]

      if opts[:type].kind_of? Fixnum
        opts[:type_id] = opts.delete :type
      elsif opts[:type].kind_of? Symbol
        opts[:type_id] = Hastur::Message.symbol_to_type_id(opts.delete(:type))
      elsif opts[:type].respond_to? :type_id
        opts[:type_id] = opts.delete(:type).type_id
      end

      raise ArgumentError.new(":type or :type_id is required") unless opts[:type_id]

      # make sure :to/:from are proper UUID's in 36-byte hex, but don't be
      # opinionated about them beyond that
      unless Hastur::Util.valid_uuid?(opts[:to])
        raise ArgumentError.new(":to => '#{opts[:to]}' is not a valid UUID")
      end
      unless Hastur::Util.valid_uuid?(opts[:from])
        raise ArgumentError.new(":from => '#{opts[:from]}' is not a valid UUID")
      end

      @version   = opts[:version]  || VERSION
      @type_id   = opts[:type_id]
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

    def type_symbol
      Hastur::Message.type_id_to_symbol(@type_id)
    end

    def type_class
      Hastur::Message.type_id_to_class(@type_id)
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

    def to_ack
      Hastur::Message::Ack.new(:data => self)
    end

    #
    # Return the envelope as a plain hash.
    #
    def to_hash
      {
        :version   => @version,
        :type_id   => @type_id,
        :to        => @to,
        :from      => @from,
        :ack       => @ack,
        :resend    => @resend,
        :sequence  => @sequence,
        :timestamp => @timestamp,
        :uptime    => @uptime,
        :hmac      => @hmac,
        :routers   => @routers,
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
end
