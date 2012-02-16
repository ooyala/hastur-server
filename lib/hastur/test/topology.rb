#!/usr/bin/env ruby

require "hastur/version"

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
    class TopologyProcessStillRunning < StandardError; end

    class Topology
      def initialize(resources={}, processes={}, opts={})
        @processes = {}
        @resources = {}

        resources.each do |name,resource|
          unless resource.kind_of? Hastur::Test::Resource::Base
            raise "Only subclasses of Hastur::Test::Resource::Base are currently supported."
          end
          @resources[name] = resource
        end

        processes.each do |name,process|
          unless process.kind_of? Hastur::Test::Process
            raise "Only Hastur::Test::Process is currently supported."
          end
          @processes[name] = process
        end

        @fully_initialized = false
        @all_stopped = true
      end

      def start_all
        @processes.each { |name,_| start name }

        # resources can run stuff, but are assumed to be safe to shut down along
        # with the topology's process and don't need to be killed like processes
        @resources.each { |k,r| r.run }

        # If we do many cycles, this will wind up getting called repeatedly.
        # The @all_stopped variable will make sure that's a really fast
        # operation.
        at_exit { stop_all }
      end

      #
      # Starts the node in the topology. Looks up the node's command
      # given that the topology hash is keyed off of the node's name.
      #
      def start name
        @all_stopped = false

        # run the command that starts up the node and store the subprocess for later manipulation
        @processes[name].run
      end

      #
      # Immediately kills a node given its topology name
      #
      def stop name
        process = @processes[name]
        process.stop
        unless process.done?
          STDERR.puts "SIGTERM to process #{name}, pid #{process.pid} failed. Sending SIGKILL ..."
          process.stop!
        end
        unless process.done?
          raise "Could not kill process (pid #{process.pid}) and command line '#{process}'"
        end
      end

      #
      # Kills all of the nodes in the topology.
      #
      def stop_all
        @processes.each { |name,_| stop name } unless @all_stopped
        @resources.each { |_,r| r.stop }
      end

      #
      # Reset all processes for restart.
      #
      def reset_all
        raise TopologyProcessStillRunning.new unless @all_stopped
        @processes.each { |_, process| process.reset }
      end

    end
  end
end

