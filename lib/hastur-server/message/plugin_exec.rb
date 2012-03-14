require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    class PluginExec < Base
      def initialize(opts)
        unless opts[:envelope]
          unless Hastur::Util.valid_uuid? opts[:to]
            raise ArgumentError.new("'to' field, '#{opts[:to]}', is not a valid UUID")
          end

          unless Hastur::Util.valid_uuid? opts[:from]
            raise ArgumentError.new("'from' field, '#{opts[:from]}', is not a valid UUID")
          end
        end

        super(opts)
      end

      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end
    end
  end
end
