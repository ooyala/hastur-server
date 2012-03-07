require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    class PluginExec < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope

        unless Hastur::Util.valid_uuid?(opts[:to])
          raise ArgumentError.new("'to' field, '#{opts[:to]}', is not a valid UUID")
        end

        if opts.has_key?(:from) and not Hastur::Util.valid_uuid?(opts[:from])
          raise ArgumentError.new("'from' field, '#{opts[:from]}', is not a valid UUID")
        end

        from = opts.delete :from
        opts[:from] = route_uuid

        super(opts)

        # If the 'from' passed in wasn't the plugin_exec route, it's probably a sink UUID so for the
        # purposes of clean routing, just move it to the "handled this" list at the end of the envelope.
        if from != opts[:from]
          @envelope.add_router from
        end
      end

      def decode
        MultiJson.decode @payload, :symbolize_keys => true
      end
    end
  end
end
