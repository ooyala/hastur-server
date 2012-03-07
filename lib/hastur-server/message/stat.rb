require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    #
    # s = Hastur::Message::Stat.new(:from => from, :payload => json_string)
    #
    # stat = Hastur::Stat.new( ... )
    # s = Hastur::Message::Stat.new(stat)
    # 
    class Stat < Base
      def initialize(opts)
        return super(opts) if opts.has_key? :envelope
        opts[:to] = route_uuid
        super(opts)
      end
    end
  end
end
