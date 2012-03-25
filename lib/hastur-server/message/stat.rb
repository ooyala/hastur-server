require 'ffi-rzmq'
require 'hastur-server/exception'
require 'hastur-server/util'

module Hastur
  module Message
    module Stat
      #
      # s = Hastur::Message::Stat.new(:from => from, :payload => json_string)
      #
      # stat = Hastur::Stat.new( ... )
      # s = Hastur::Message::Stat.new(stat)
      #
      class Generic < Base
        def initialize(opts)
          opts[:to] ||= '00000000-0000-0000-0000-000000000000'
          super(opts)
        end
      end

      class Mark < Generic
      end
      class Gauge < Generic
      end
      class Counter < Generic
      end
    end
  end
end
