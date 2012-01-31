#!/usr/bin/env ruby

#
# Module to help build a Hastur topology on a single machine. All pieces of the topology
# that run in subprocesses will be referencable through this wrapper.
#
module Hastur
  module Test
    module Topology
      attr_accessor :processes, :topology

      def initialize
        @processes = Hash.new   # stores process information of the running node
        @topology = Hash.new    # stores the metadata for all of the nodes in the topology
      end
     
      # 
      # For each of the nodes in the topology, store it in a hash where
      # the key is the node name and the value is the actual node itself
      #
      def build topology
        topology.each do |n|
          @topology[n[:name]] = n
        end
      end

      #
      # Starts the node in the topology. Looks up the node's command
      # given that the topology hash is keyed off of the node's name.
      #
      def start name
        # run the command that starts up the node and store the subprocess for later manipulation
        @processes[name] = IO.popen(@topology[name][:command])
      end

      #
      # Starts all of the nodes in the topology in undeterministic order.
      #
      def start_all
        @topology.each do |node|
          start node[:name]
        end
      end
    end
  end
end

