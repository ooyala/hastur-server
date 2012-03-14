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
        opts[:ack] = true unless opts.has_key?(:ack)
        opts[:to]  ||= '00000000-0000-0000-0000-000000000000'
        super(opts)
      end
    end
  end
end
