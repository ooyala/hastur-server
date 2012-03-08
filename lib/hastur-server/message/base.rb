require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
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
        # automatically construct an envelope if :from & :to are provided (all flags passed through)
        elsif not opts[:envelope] and opts[:from] and opts[:to]
          @envelope = Hastur::Envelope.new opts.merge({:type => type_id})
        else
          raise ArgumentError.new ":envelope or :from/:to arguments are required."
        end

        if opts[:data]
          raise ArgumentError.new ":data must respond to :to_hash" unless opts[:data].respond_to?(:to_hash)
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
      # Return the type ID for the class.
      # ID's are defined in the Hastur::Message module.
      #
      def self.type_id() CLASS_TYPE_IDS[self] end
      def type_id() CLASS_TYPE_IDS[self.class] end

      #
      # Return the route UUID for the class.
      # Route UUID's are defined in the Hastur::Message module.
      #
      def self.route_uuid() CLASS_ROUTE_UUIDS[self] end
      def route_uuid() CLASS_ROUTE_UUIDS[self.class] end

      #
      # Return the symbol that should be used for the class.
      # The symbols are defined in the Hastur::Message module.
      #
      def self.type_symbol() CLASS_SYMBOLS[self] end
      def type_symbol() CLASS_SYMBOLS[self.class] end

      #
      # WARNING: send() is going to be renamed to "transmit" soon! (al, 2012-03-06)
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

        if opts[:secret]
          @envelope.update_hmac(@payload, opts[:secret])
        end

        messages << ZMQ::Message.new(@envelope.pack)
        messages << ZMQ::Message.new(@payload.to_s)

        rc = socket.sendmsgs messages
        messages.each { |m| m.close }
        @zmq_parts = [] if opts[:final]
        @envelope.incr_resend # automatically bump the send count in case this message is resent
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
  end
end