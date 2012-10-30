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
    class Error < Simple
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
      # @param [Object] data to include in the message payload
      # @return [String] JSON-encoded data
      #
      def encode(data)
        out = { :error => :unset, :class => data.class.to_s, :data => data }

        case data
          when Hastur::Message::Base
            out[:error] = :message
            out[:data] = data.to_hash
          when Exception
            out[:error] = :exception
            out[:data] = data.inspect
            out[:backtrace] = data.backtrace rescue []
          when Hash
            out[:error] = data.has_key?(:error) ? data.delete(:error) : :structured
          when Array
            out[:error] = :structured
          when String
            out[:error] = :raw
          else
            out[:error] = :undefined
            out[:data] = data.inspect
        end

        @payload = MultiJson.dump(out)
      end
    end
  end
end
