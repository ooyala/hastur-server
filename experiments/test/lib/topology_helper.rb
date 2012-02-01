#!/usr/bin/env ruby

# Load local Hastur first
$LOAD_PATH.unshift "../../lib"

require "hastur/version"
require "erubis"

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

        @processes.each do |name, |
          start name
        end
      end

      #
      # Starts the node in the topology. Looks up the node's command
      # given that the topology hash is keyed off of the node's name.
      #
      def start name
        allocate_resources

        # run the command that starts up the node and store the subprocess for later manipulation
        @processes[name][:io] = IO.popen(@processes[name][:expanded_command])
        puts @processes[name].inspect
      end

      #
      # Immediately kills a node given its topology name
      #
      def stop name
        pid = @processes[name][:io].pid
        if pid
          Process.kill(TERM, pid)
          Process.waitpid(pid, Process::WHOHANG)
        end

        # Should we read and save stdout/stderr?
        @processes[name][:io] = nil
      end

      #
      # Kills all of the nodes in the topology.
      #
      def stop_all
        @processes.each do |name, |
          stop name
        end
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
        eruby = Erubis::ERuby.new command
        eruby.evaluate locals
      end

      REQUIRED_NODE_KEYS = [ :name, :command ]

      def verify_process(node)
        unless (REQUIRED_NODE_KEYS - node.keys).empty?
          raise "This node is missing key(s) #{(REQUIRED_NODE_KEYS - node.keys).join(', ')}: #{node.inspect}"
        end

        node[:resources] ||= node["resources"]
        node[:resources] ||= {}

        Topology.plugins.values.each do |plugin|
          plugin.verify_process(node) if plugin.respond_to?(:verify_process)
        end
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

        Topology.plugins.values.each do |plugin|
          plugin.allocate_resources(@processes) if plugin.respond_to?(:allocate_resources)
        end

        @processes.each do |_, process|
          process[:expanded_command] = expand_text(command, process[:variables])
        end

        @fully_initialized = true
      end

      def free_resources
        Topology.plugins.values.each do |plugin|
          plugin.free_resources(@processes) if plugin.respond_to?(:free_resources)
        end
        @fully_initialized = false
      end

      module ZMQ
        def context
          @context ||= ZMQ::Context.new
        end

        def port_open?(port_num)
          begin
            s = TCPServer.new port_num
            true
          rescue
            false
          end
        end

        def mutex
          @mutex ||= Mutex
        end

        def capture_packet_to(packet, to)
          mutex.synchronize do
            @packet_captures_to ||= {}
            @packet_captures_to[to] ||= []
            @packet_captures_to[to] << packet

            @packet_listeners_to ||= {}
            (@packet_listeners_to[to] || []).each do |listener_block|
              listener_block.call(packet, :from => from, :to => to)
            end
          end
        end

        def listen_for_packets_to(to, &block)
          mutex.synchonize do
            @packet_listeners_to ||= {}
            @packet_listeners_to[to] ||= []
            @packet_listeners_to[to] << block
          end
        end

        def all_packets_to(to)
          mutex.synchonize do
            @packet_captures_to ||= {}
            (@packet_captures_to[to] || []).dup
          end
        end

        # Running multiple test harnesses?  Start the ports at different offsets.
        def start_ports_at(port)
          @last_port_num = port
        end

        def allocate_port
          @last_port_num ||= 21000

          attempts = 0
          while attempts < 10
            @last_port_num += 1
            return @last_port_num if port_open?(@last_port_num)
            attempts += 1
          end

          raise "Couldn't find an open TCP port after 10 attempts!"
        end

        # For each ZMQ port type, we receive on the actual port type
        # and resend on a corresponding port type.
        SEND_PORT_FOR = {
          :req => :rep,
          :rep => :req,
          :push => :pull,
          :pull => :push,
          :pub => :sub,
          :sub => :pub,
          :router => nil,
          :dealer => :req,
        }

        def allocate_resources(processes)
          processes.each do |_,process|
            zmq = process[:resources][:zmq]
            next unless zmq

            zmq.each do |socket|
              socket[:forwarder_thread] = Thread.new do
                type = socket[:type]
                uri_in = "tcp://127.0.0.1:#{allocate_port}"
                uri_out = "tcp://127.0.0.1:#{socket[:listen]}"

                # Set HWM to 1 so we don't get "instant send" on one end and everything backed
                # up here.
                incoming = bind_socket(context, type, uri_in, :hwm => 1)
                outgoing = connect_socket(context, SEND_PORT_FOR[type], uri_out, :hwm => 1)

                loop do
                  message = multi_recv(incoming)

                  capture_packet_to(message, uri_out)

                  if socket[:type] == :router
                    # Remove the extra envelope section added by receiving on a router socket
                    client_id = message.shift

                    @router_sockets ||= {}
                    @router_sockets[client_id] ||= connect_socket(context, SEND_PORT_FOR[type],
                                                                  uri_out, :hwm => 1,
                                                                  :identity => client_id)
                    outgoing = @router_sockets[client_id]
                  end

                  multi_send(outgoing, message)
                end
              end
            end
          end
        end

        def free_resources(processes)
          processes.each do |_,process|
            zmq = process[:resources][:zmq]
            next unless zmq

            zmq.each do |socket|
              Thread.kill socket[:forwarder_thread]
            end
          end
        end
      end
    end
  end
end

Hastur::Test::Topology.register_plugin(:zmq, Hastur::Test::Topology::ZMQ)
