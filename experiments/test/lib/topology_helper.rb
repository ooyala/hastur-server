#!/usr/bin/env ruby

#
# Module to help build a Hastur topology on a single machine. All pieces of the topology
# that run in subprocesses will be referencable through this wrapper.
#
module Hastur
  module Test
    module Topology
      attr_accessor :processes

      def initialize
        @processes = Hash.new
      end
     
      # 
      # Executes the command for each given node in the topology
      #
      def build topology
        topology.each do |node|
          # TODO(viet): run the command that starts up the node

          # TODO(viet): store the subprocess for later manipulation

        end
      end
    end
  end
end

