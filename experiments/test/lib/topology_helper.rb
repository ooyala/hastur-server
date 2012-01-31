#!/usr/bin/env ruby

# Load local Hastur first
$LOAD_PATH.unshift "../../lib"

require "hastur/version"
require "erubis"

def expand_command(command, locals => {})
  eruby = Erubis::ERuby.new command
  eruby.evaluate locals.merge(:version => Hastur::VERSION)
end

# To test, you'll be creating a Topology, representing a cluster of
# interconnected processes.  You'll also optionally declare a number
# of resources for the test framework to verify - files it can read,
# network connections it can snoop or spoof and so on.  By declaring
# these resources, you gain the ability to make assertions against
# them.

# After creating the Topology and adding processes to it, you run it.
# When you do, the framework will allocate resources and rework the
# command line of every node to use the resources that the framework
# has allocated, faked or mocked.  For instance, for a ZeroMQ socket
# the framework will create an identical forwarding socket that
# records traffic before resending to the application's actual socket.

# Since the test framework doesn't know the command line of every
# possible executable, you'll need to write your command lines in
# terms of those resources.  Erb is used to let you do logic in the
# command-line declarations, and variables are passed in for the
# resources that the test framework has created.


#
# Module to help build a Hastur topology on a single machine. All pieces of the topology
# that run in subprocesses will be referenceable through this wrapper.
#
module Hastur
  module Test
    module Topology
      def initialize(processlist = [])
        @processes = Hash.new   # stores process information of the running node
        @topology = Hash.new    # stores the metadata for all of the nodes in the topology

        # Add processes, if supplied
        if processlist.respond_to?(:each)
          add_processes processlist
        else
          add_process processlist
        end

        @fully_initialized = false
      end

      # Read-only accessor
      def processes
        @processes.dup
      end

      # Read-only accessor
      def topology
        @topology.dup
      end

      def add_process(process)
        verify_process process

        @processes[process[:name]] = process

        @fully_initialized = nil
      end

      def add_processes(list)
        list.each { |n| add_process(n) }
      end

      def start_all
        unless @fully_initialized
          allocate_resources
          update_processes_for_resources
          @fully_initialized = true
        end

        @topology.each do |node|
          start node[:name]
        end
      end

      #
      # Starts the node in the topology. Looks up the node's command
      # given that the topology hash is keyed off of the node's name.
      #
      def start name
        # run the command that starts up the node and store the subprocess for later manipulation
        @processes[name] = IO.popen(@topology[name][:command]) unless @topology[name].nil?
      end

      #
      # Immediately kills a node given its topology name
      #
      def stop name
        pid = @processes[name].pid
        if pid
          Process.kill(TERM, pid)
          Process.waitpid(pid, Process::WHOHANG)
        end
      end

      #
      # Kills all of the nodes in the topology.
      #
      def stop_all
        @processes.each do |name, node|
          stop name
        end
      end

      private

      REQUIRED_NODE_KEYS = [ :name, :command ]

      def verify_node(node)
        unless (REQUIRED_NODE_KEYS - node.keys).empty?
          raise "This node is missing key(s) #{(REQUIRED_NODE_KEYS - node.keys).join(', ')}: #{node.inspect}"
        end

        node[:resources] ||= node["resources"]
        node[:resources] ||= {}
      end

      def allocate_resources

      end

      def update_processes_for_resources
        # Create space in topology map for cached information
        @topology = {}
        @processes.each do |name, process|
          @topology[name] = process.dup
        end
      end
    end
  end
end

