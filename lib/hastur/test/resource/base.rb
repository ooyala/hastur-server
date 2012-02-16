require 'ffi-rzmq'

module Hastur
  module Test
    module Resource
      SYNC = Mutex.new
      def self.synchronize(&block)
        SYNC.synchronize(&block)
      end
      class Base
        attr_reader :readers, :writers, :input, :output, :running

        def initialize(opts)
          @unlinks ||= []
          @readers ||= []
          @writers ||= []
          @input   ||= []
          @output  ||= []

          add_reader(opts[:reader]) if opts[:reader]
          if opts[:readers].respond_to? :each
            opts[:readers].each { |a| add_action(@readers, a) }
          end

          add_writer(opts[:writer]) if opts[:writer]
          if opts[:writers].respond_to? :each
            opts[:writers].each { |a| add_action(@writers, a) }
          end
        end

        def run
        end

        #
        # Cleans up any files registered in @unlinks. Child classes should call super().
        #
        def stop
          @unlinks.each do |file|
            File.unlink(file) if File.socket?(file)
          end
        end

        #
        # Add a writer action. Can be a block which will be executed, with its output emitted
        # to the target, a list of things to write to the target, :ignore or nil (which is ignored).
        #
        def add_writer(action=nil, &block)
          if block_given?
            @writers << block
          end

          if action.respond_to? :call
            @writers << action
          elsif action == :ignore or action.nil?
            # nothing to do here
          else
            raise ArgumentError.new "Invalid add_writer class: #{action.class}"
          end
        end

        #
        # Add a reader action. Can be a block which will be executed for each unit of input, :capture
        # to capture all items emitted by the target to a list (accessible with .output), :ignore, or
        # nil (which will be ignored).
        #
        def add_reader(action=nil, &block)
          if block_given?
            @readers << block
          end

          if action.respond_to? :call
            @readers << action
          elsif action == :capture
            @readers << proc { |item| @output.push(item) }
          elsif action == :drain and @readers.empty?
            @readers << proc { |item| item } # make sure there's at least one proc so recvmsg gets run
          elsif action == :ignore or action.nil?
            # nothing to do here
          else
            raise ArgumentError.new "Invalid add_reader class: #{action.class}"
          end
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
