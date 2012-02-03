#!/usr/bin/env ruby

require "hastur/version"
require "erubis"

require_relative "topology_zmq_helper"

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
    class Topology
      def initialize(processlist = [])
        @processes = Hash.new   # stores process information for all nodes

        # Add processes, if supplied
        if processlist.respond_to?(:each)
          add_processes processlist
        else
          add_process processlist
        end

        @fully_initialized = false
        @all_stopped = true
      end

      def process_names
        @processes.keys
      end

      # Read-only accessor
      def processes
        @processes.dup # dup so nobody can modify it
      end

      def add_process(process)
        verify_process process

        @processes[process[:name]] = process.dup

        @fully_initialized = nil
      end

      def add_processes(list)
        list.each { |n| add_process(n) }
      end

      def start_all
        allocate_resources

        @processes.each { |name, | start name }

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
        allocate_resources

        # TODO(noah): redirect and save stdout and stderr
        # TODO(noah): redirect and pipe in stdin for test input
        # TODO(noah): allow passing in spawn-type options like resource limits

        # run the command that starts up the node and store the subprocess for later manipulation
        STDERR.puts "Running process: #{@processes[name][:expanded_command]}"
        @processes[name][:pid] = spawn(@processes[name][:expanded_command])
      end

      #
      # Immediately kills a node given its topology name
      #
      def stop name
        return unless @processes[name][:pid]

        pid = @processes[name][:pid]
        Process.kill("TERM", pid)
        sleep 0.01
        ret = Process.waitpid(pid, Process::WNOHANG)
        unless ret == pid
          STDERR.puts "Sending kill -9 to #{name}, pid #{pid}!"
          begin
            Process.kill(-9, pid)
          rescue
            STDERR.puts "Exception killing #{name} process (#{pid}): #{$!.message}"
          end
        end

        @processes[name][:pid] = nil
      end

      #
      # Kills all of the nodes in the topology.
      #
      def stop_all
        return if @all_stopped

        @processes.each { |name, | stop name }
      end

      def self.register_plugin(name, klass)
        @plugins ||= {}
        raise "Plugin for #{name} already exists in Hastur::Test::Topology!" if @plugins[name]
        @plugins[name] = klass
      end

      def self.plugins
        @plugins ||= {}
        @plugins.dup
      end

      private

      def expand_text(command, locals = {})
        return command.map {|c| expand_text(c, locals) } if command.kind_of?(Array)

        begin
          eruby = Erubis::Eruby.new command
          eruby.result locals
        rescue
          STDERR.puts "Error evaluating in erubis!"
          STDERR.puts "Text: #{command}"
          STDERR.puts "Variables: #{locals.inspect}"

          raise
        end
      end

      REQUIRED_NODE_KEYS = [ :name, :command ]

      def self.send_to_plugins(method, *args, &block)
        plugins.values.each { |plugin| plugin.send(method, *args, &block) if plugin.respond_to?(method) }
      end

      def verify_process(node)
        unless (REQUIRED_NODE_KEYS - node.keys).empty?
          raise "This node is missing key(s) #{(REQUIRED_NODE_KEYS - node.keys).join(', ')}: #{node.inspect}"
        end

        node[:resources] ||= node["resources"]
        node[:resources] ||= {}

        Topology.send_to_plugins(:verify_process, node)
      end

      def allocate_resources
        return if @fully_initialized

        stop_all

        @processes.each do |name, process|
          process[:variables] = {
            :name => name,
            :process => process,
            :version => Hastur::VERSION,
          }
        end

        Topology.send_to_plugins(:allocate_resources, @processes)

        @processes.each do |_, process|
          process[:expanded_command] = expand_text(process[:command], process[:variables])
        end

        @fully_initialized = true
      end

      def free_resources
        Topology.send_to_plugins(:free_resources, @processes)
        @fully_initialized = false
      end
    end
  end
end

Hastur::Test::Topology.register_plugin(:zmq, Hastur::Test::ZMQ)
