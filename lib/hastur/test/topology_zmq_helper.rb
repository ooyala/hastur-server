require "socket"
require "ffi-rzmq"
require "multi_json"
require "hastur/zmq_utils"

module Hastur
  module Test
    module ZMQ
      extend self

      def context
        @context ||= ::ZMQ::Context.new
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
        @mutex ||= Mutex.new
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
        mutex.synchronize do
          @packet_listeners_to ||= {}
          @packet_listeners_to[to] ||= []
          @packet_listeners_to[to] << block
        end
      end

      def all_packet_receivers
        mutex.synchronize do
          @packet_captures_to ||= {}
          @packet_captures_to.keys
        end
      end

      def all_packets_to(to)
        mutex.synchronize do
          @packet_captures_to ||= {}

          if to == :all
            @packet_captures_to.values.inject([], &:+)
          else
            (@packet_captures_to[to] || []).dup
          end
        end
      end

      #
      # Retrieves all of the payloads from raw messages that came to this particular socket.
      #
      def all_payloads_to(to)
        messages = all_packets_to(to)
        0.upto(messages.size - 1) do |idx|
          messages[idx] = MultiJson.decode messages[idx][-1]
        end
        messages
      end

      # Running multiple test harnesses?  Start the ports at different offsets.
      def start_ports_at(port)
        @last_port_num = port
      end

      def allocate_port
        mutex.synchronize do
          @last_port_num ||= 21000

          attempts = 0
          while attempts < 10
            @last_port_num += 1
            return @last_port_num if port_open?(@last_port_num)
            attempts += 1
          end

          raise "Couldn't find an open TCP port after 10 attempts!"
        end
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

        # Looks like both dealer and router sockets are going to require a bit of weird special-casing
        :router => :dealer,
        :dealer => :router,
      }

      def allocate_resources(processes)
        puts "Allocating ZMQ resources for #{processes.keys} ... "
        processes.each do |_, process|
          process[:variables][:zmq] = {}
        end

        all_sockets = {}

        processes.each do |_, process|
          zmq = process[:resources][:zmq]
          next unless zmq
          zmq.each do |socket|
            socket[:forwarder_port] = allocate_port
            socket[:forwarder_thread] = Thread.new do
              begin
                forward_packets(socket)
              rescue
                STDERR.puts "\n   ---> Exception killed forwarding thread: #{$!.message}\n#{$!.backtrace.join("\n")}"
              end
            end
            # This process gets the URI of its own sockets, unmodified
            socket_uri = "tcp://127.0.0.1:#{socket[:listen]}"

            STDERR.puts "Setting variable zmq[:#{socket[:name]}] for process #{process[:name]}..."
            process[:variables][:zmq][socket[:name]] = socket_uri

            raise "Duplicate socket name #{socket[:name]} between processes!" if all_sockets[socket[:name]]
            all_sockets[socket[:name]] = "tcp://127.0.0.1:#{socket[:forwarder_port]}"
            # This is important so that all of the forwarders fire up in order. Otherwise messages could get dropped/stuck 
            # in between nodes. This will lead to a gridlock situation because HWM is only at 1.
            #
            # TODO(viet): Perhaps this should be a configurable.
            sleep 1
          end
        end

        processes.each do |_, process|
          all_sockets.each do |socket_name, socket_uri|
            # Each process sees the URI of *other* processes' sockets with the forwarding URI
            process[:variables][:zmq][socket_name] ||= socket_uri
          end
        end
      end

      def free_resources(processes)
        processes.each do |_,process|
          zmq = process[:resources][:zmq]
          next unless zmq

          zmq.each do |socket|
            Thread.kill socket[:forwarder_thread]
            socket[:forwarder_port] = nil
            socket[:forwarder_thread] = nil
          end
        end
      end

      # This is a hack.  It's going to be difficult for us to deal
      # with router sockets, in the sense that they look for a
      # specific socket identity to send to and we don't necessarily
      # know who is connected to us.  Right now, we start
      # impersonating a given socket connected to a router after the
      # first time it sends to us.  Which is good enough... sometimes.

      # I have no doubt that there is a better ZMQ hack to make this
      # work, but I'm pretty sure it's impossible to do correctly
      # using the supported public API.  On the plus side, the
      # "correct" way to do it is to send to the router first, and
      # that works fine.

      def forward_packets(socket)
        type = socket[:type]
        uri_in = "tcp://127.0.0.1:#{socket[:forwarder_port]}"
        uri_out = "tcp://127.0.0.1:#{socket[:listen]}"
        # Set HWM to 1 so we don't get "instant send" on one end and everything backed
        # up here.
        incoming = ZMQUtils.bind_socket(context, type.to_s, uri_in, :hwm => 1)
        outgoing = ZMQUtils.connect_socket(context, SEND_PORT_FOR[type].to_s, uri_out, :hwm => 1)
        poller = ::ZMQ::Poller.new
        poller.register_readable incoming
        poller.register_readable outgoing
        loop do
          poller.poll 0.1
          if poller.readables.include?(outgoing)
            message = ZMQUtils.multi_recv(outgoing)
            # TODO: Should we keep this? Not really sure how people will know of the dynamically
            #       constructed URI to access it
            capture_packet_to(message, uri_in)
            capture_packet_to(message, socket[:name])
            ZMQUtils.multi_send(incoming, message)
          end
          unless (poller.readables - [outgoing]).empty?
            message = ZMQUtils.multi_recv(incoming)
            # TODO: Should we keep this? Not really sure how people will know of the dynamically
            #       constructed URI to access it
            capture_packet_to(message, uri_out)
            if socket[:type] == :router
              # Remove the extra envelope section added by receiving on a router socket
              client_id = message.shift
              @router_sockets ||= {}
              @router_sockets[client_id] ||= ZMQUtils.connect_socket(context, SEND_PORT_FOR[type],
                                                                     uri_out, :hwm => 1,
                                                                     :identity => client_id)
              outgoing = @router_sockets[client_id]
              poller.register_readable(outgoing)
            elsif socket[:type] == :dealer
              # TODO: Not sure what to do here
            end
            ZMQUtils.multi_send(outgoing, message)
          end
        end
      end

      #
      # Resets the Topology ZMQ Helper as if it was brand new.
      #
      def reset
        @packet_captures_to.clear
        @last_port_num = 21000
      end
    end
  end
end
