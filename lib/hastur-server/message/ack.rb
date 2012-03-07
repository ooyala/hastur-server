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
  end
end
