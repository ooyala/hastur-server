require "socket"

module Hastur
  module Test
    module ZMQ
      extend self

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
        :router => :req,
        :dealer => :req,
      }

      def allocate_resources(processes)
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
              forward_packets(socket)
            end

            # This process gets the URI of its own sockets, unmodified
            socket_uri = "tcp://127.0.0.1:#{socket[:listen]}"

            STDERR.puts "Setting variable zmq[:#{socket[:name]}] for process #{process[:name]}..."
            process[:variables][:zmq][socket[:name]] = socket_uri

            raise "Duplicate socket name #{socket[:name]} between processes!" if all_sockets[socket[:name]]
            all_sockets[socket[:name]] = "tcp://127.0.0.1:#{socket[:forwarder_port]}"
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

      def forward_packets(socket)
        type = socket[:type]
        uri_in = "tcp://127.0.0.1:#{socket[:forwarder_port]}"
        uri_out = "tcp://127.0.0.1:#{socket[:listen]}"

        # Set HWM to 1 so we don't get "instant send" on one end and everything backed
        # up here.
        incoming = bind_socket(context, type, uri_in, :hwm => 1)
        outgoing = connect_socket(context, SEND_PORT_FOR[type], uri_out, :hwm => 1)

        poller = ZMQ::Poller.new
        poller.register_readable incoming
        poller.register_readable outgoing

        loop do
          poller.poll 0.1

          if poller.readables.include?(outgoing)
            message = multi_recv(outgoing)
            capture_packet_to(message, uri_in)
            multi_send(incoming, message)
          end

          if poller.readables.include?(incoming)
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
end