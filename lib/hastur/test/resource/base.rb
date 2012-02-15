require 'ffi-rzmq'

module Hastur
  module Test
    module Resource
      class Base
        attr_reader :actions, :items

        def initialize(opts, &block)
          @actions = []

          add_action(opts[:action]) if opts[:action]
          if opts[:actions]
            opts[:actions].each { |a| add_action(a) }
          end

          if block_given?
            add_action(block)
          end
        end

        def stop
        end

        def add_action(action)
          case action
            when Proc
              @actions << action
            when :capture
              @actions << proc { |item| @items << item }
            when :drain
            when :ignore
            when nil
              # nothing to do here
            else
              raise ArgumentError.new "Invalid action class: #{action.class}"
          end
        end
 
        def to_s
          raise "Subclass appears to have forgotten to implement to_s."
        end

        def process_item(item)
          @actions.each do |action|
            action.call(item)
          end
        end
      end
    end
  end
end
