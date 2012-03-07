require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    #
    # m = Hastur::Message::Log.new :from => from, :payload => string
    #
    class Log < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = route_uuid
        super(opts)
      end
    end
  end
end
