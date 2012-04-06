module Hastur
  module Message
    # Commands (usually) flow from the core out to the edge.
    module Cmd
      class PluginV1 < Base
        #
        # Create a new command message.
        # @option [String] :to 36-byte UUID
        # @option [String] :from 36-byte UUID
        #
        def initialize(opts)
          raise ArgumentError.new "Only hash arguments are supported." unless opts.kind_of? Hash

          unless opts[:envelope]
            unless Hastur::Util.valid_uuid? opts[:to]
              raise ArgumentError.new("'to' field, '#{opts[:to]}', is not a valid UUID")
            end

            unless Hastur::Util.valid_uuid? opts[:from]
              raise ArgumentError.new("'from' field, '#{opts[:from]}', is not a valid UUID")
            end
          end

          super(opts)
        end
      end
    end
  end
end
