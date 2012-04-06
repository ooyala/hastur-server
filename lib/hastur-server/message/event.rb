module Hastur
  module Message
    class Event < Simple
      def initialize(opts)
        raise ArgumentError.new "Only hash arguments are supported." unless opts.kind_of? Hash
        opts[:ack] = true unless opts.has_key?(:ack)
        super(opts)
      end
    end
  end
end
