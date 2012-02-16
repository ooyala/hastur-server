require 'hastur/test/resource/base'
require 'rainbow'

module Hastur
  module Test
    module Resource
      #
      # a simple colored output resource
      #
      # e.g. Hastur::Test::Resource::Tty.new(:fg => :green)
      # Hastur::Test::Resource::Tty.new(:fg => :green, :bg => :white)
      #
      class Tty < Hastur::Test::Resource::Base
        def initialize(opts={}, &block)
          opts[:action] = proc { |line| puts line }
          if opts[:fg] and opts[:bg]
            opts[:action] = proc { |line| puts line.foreground(opts[:fg]).background(opts[:bg]) }
          elsif opts[:fg]
            opts[:action] = proc { |line| puts line.foreground(opts[:fg]) }
          elsif opts[:bg]
            opts[:action] = proc { |line| puts line.background(opts[:bg]) }
          end
          super(opts, &block)
        end
      end
    end
  end
end
