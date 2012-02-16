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
        def initialize(opts={})
          super(opts)

          if opts[:fg] and opts[:bg]
            add_reader { |line| puts line.foreground(opts[:fg]).background(opts[:bg]) }
          elsif opts[:fg]
            add_reader proc { |line| puts line.foreground(opts[:fg]) }
          elsif opts[:bg]
            add_reader proc { |line| puts line.background(opts[:bg]) }
          else
            add_reader { |line| puts line }
          end
        end
      end
    end
  end
end
