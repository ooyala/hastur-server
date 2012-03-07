require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    #
    # a general event
    #
    class Event < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = route_uuid
        opts[:ack] = true
        super(opts)
      end
    end
  end
end
