require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
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
    class Error < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = route_uuid

        if opts[:data]
          opts[:payload] = encode(opts.delete(:data))
        end

        super(opts)
      end

      #
      # Always JSON encode any data in an Error, because it may be malformed
      # or even (accidentally) malicious data.  The transmitted JSON should always
      # have two keys, :error and :data. :data could contain anything, including
      # more JSON.
      # 
      # A few automatic conversions are executed:
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
            data = data.to_hash
          when Exception
            error = :exception
            data = data.inspect
          when Hash
            error = data.has_key?(:error) ? data.delete(:error) : :structured
          when Array
            error = :structured
          when String
            error = :raw
            data = opts.delete(:data)
          else
            error = :undefined
            data = opts.delete(:data).inspect
        end

        @payload = MultiJson.encode({:error => error, :data  => data})
      end
    end
  end
end
