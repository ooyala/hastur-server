require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    #
    # generally the ack_id will the the ack'ed message's envelope
    # e.g. ack = Hastur::Message::Ack.new :from => uuid, :data => msg.envelope
    # the destination UUID is automatically extracted from the envelope
    #
    # Or, just call msg.to_ack.send(socket). (implemented in Hastur::Message::Base)
    #
    class Ack < Base
      #
      # Create a new Ack. Must be created from another message's envelope.
      # @param [Hash{Symbol => String,Hastur::Envelope}] opts
      # @option opts [String] :to system expecting the ack
      # @option opts [String] :from system sending the ack
      # @option opts [Hastur::Envelope] :data Hastur Envelope object the ack is for
      # @option opts [String] :payload a packed Envelope
      #
      def initialize(opts)
        if opts.has_key? :data
          data = opts[:data]
        elsif opts.has_key? :payload
          data = decode opts[:payload]
        end

        unless data.kind_of? Hastur::Envelope
          raise ArgumentError.new "acks can only be created from Hastur::Envelope objects"
        end

        unless opts.has_key? :envelope
          opts[:to]   = opts[:from] || data.from
          opts[:from] = opts[:to]   || data.to
          # disable acks in an ack, acking an ack doesn't make a lot of sense
          opts[:ack]  = false
        end

        unless opts.has_key? :payload
          opts[:payload] = encode data
          opts.delete :data
        end

        super(opts)
      end

      #
      # Return the envelope of the acked message.
      #
      def acked
        decode
      end

      def encode(data)
        @payload = data.pack
      end

      def decode(payload=@payload)
        Hastur::Envelope.parse payload
      end
    end
  end
end
