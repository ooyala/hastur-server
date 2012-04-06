module Hastur
  module Message
    class Noop < Base

      def initialize(opts)
        raise ArgumentError.new "Only hash arguments are supported." unless opts.kind_of? Hash
        opts[:to] ||= '00000000-0000-0000-0000-000000000000'
        opts[:payload] = '{}'
        super(opts)
      end

      def decode(data=nil)
        {}
      end

      def encode
        @payload = '{}'
      end
    end
  end
end
