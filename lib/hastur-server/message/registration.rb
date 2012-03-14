require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    class Registration < Base
      def initialize(opts)
        opts[:to] ||= '00000000-0000-0000-0000-000000000000'
        super(opts)
      end
    end
  end
end
