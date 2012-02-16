require 'hastur/util'
require 'hastur/test/resource/base'

module Hastur
  module Test
    module Resource
      class UnixSocket < Hastur::Test::Resource::Base
        attr_reader :path

        def initialize(opts={})
          super(opts)
          @path = "#{::Process.pid}-#{Hastur::Util.next_seq}"
          @unlinks << @path
        end

        def to_s
          @path
        end
      end
    end
  end
end

