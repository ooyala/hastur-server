require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    class Heartbeat < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = route_uuid
        super(opts)
      end
    end
  end
end
