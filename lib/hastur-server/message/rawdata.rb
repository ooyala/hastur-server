require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    class Rawdata < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = route_uuid
        raise ArgumentError.new "Rawdata only supports Strings" unless opts[:payload].kind_of? String
        raise ArgumentError.new "Rawdata only supports raw payloads, e.g. :payload => 'stuff'" if opts[:data]
        super(opts)
      end

      #
      # Update payload. Rawdata never modifies the payload, even if (especially if) it's binary.
      #
      def encode(data)
        @payload = data
      end

      #
      # Stub. Does nothing but return the payload.
      #
      def decode
        @payload
      end
    end
  end
end
