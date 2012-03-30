require "ffi-rzmq"
require "celluloid"
require "hastur-server/message"
require "hastur-server/router"

module Hastur
  class Service
    class CoreRouter
      include Celluloid

      attr_reader :router_uri, :incoming_uri, :outgoing_uri

      #
      # Create a new core router.
      # @param [String] uuid the core router's UUID (36 byte hyphenated string)
      # @param [Hash{Symbol => String}] opts
      # @option [String] :router_uri default tcp://*:8126
      # @option [String] :incoming_uri default tcp://*:8127
      # @option [String] :outgoing_uri default tcp://*:8128
      #
      def initialize(uuid, opts={})
        @ctx = ::ZMQ::Context.new

        @router_uri   = opts[:router_uri]   || "tcp://*:8126"
        @incoming_uri = opts[:incoming_uri] || "tcp://*:8127"
        @outgoing_uri = opts[:outgoing_uri] || "tcp://*:8128"

        @router_socket   = @ctx.socket(::ZMQ::ROUTER)
        @incoming_socket = @ctx.socket(::ZMQ::PUSH)
        @outgoing_socket = @ctx.socket(::ZMQ::PULL)

        setsockopts(@router_socket)
        setsockopts(@incoming_socket)
        setsockopts(@outgoing_socket)

        bind(@router_socket,   @router_uri)
        bind(@incoming_socket, @incoming_uri)
        bind(@outgoing_socket, @outgoing_uri)

        # agent UUID to ZMQ envelope ID mapping cache
        @agents = {}
      end

      def run
        while @running
          poll
        end
      end

      def stop
        @running = false
        @router_socket.close
        @incoming_socket.close
        @outgoing_socket.close
        @ctx.terminate
      end

      private

      def poll
        rc = @poller.poll 1
        if ::ZMQ::Util.resultcode_ok? rc
          @poller.readables.each do |r|
            if r == @router_socket
              forward_router_to_incoming
            elsif r == @outgoing_socket
              forward_outgoing_to_router
            end
          end
        else
          send_error ::ZMQ::Util.error_string
        end
      end

      def read_from(socket)
        message = []
        rc = socket.recvmsgs message
        if ::ZMQ::Util.resultcode_ok? rc
          message
        else
          send_error ::ZMQ::Util.error_string
          false
        end
      end

      def send_to(socket, message)
        rc = socket.sendmsgs message
        if ::ZMQ::Util.resultcode_ok? rc
          true
        else
          send_error ::ZMQ::Util.error_string
          false
        end
      end

      def forward_router_to_incoming
        message = read_from @router_socket

        if message
          # cache the ZMQ envelope to route messages back to agents
          envelope = Hastur::Envelope.parse messages[-2].copy_out_string
          @agents[envelope.from] = messages[0].copy_out_string

          # forward the message, sans the ZMQ envelope
          send_to @incoming_socket, message[1..2]

          # close all of the zmq messages or we might leak C memory
          messages.each do |m| m.close end
        end
      end

      def forward_outgoing_to_router
        message = read_from @outgoing_socket

        if message
          envelope = Hastur::Envelope.parse messages[-2].copy_out_string
          agent_id = @agents[envelope.from]
          message.unshift ::ZMQ::Message.new(agent_id)
          send_to @router_socket, message
        end
      end

      def setsockopts(sock)
        rc = sock.setsockopt(::ZMQ::LINGER, -1)
        raise "Error setting ZMQ::LINGER: #{::ZMQ::Util.error_string}" unless rc > -1
        rc = sock.setsockopt(::ZMQ::HWM, 1)
        raise "Error setting ZMQ::HWM: #{::ZMQ::Util.error_string}" unless rc > -1
      end

      def bind(sock, uri)
        rc = sock.bind(uri)
        raise "Could not bind socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end
  end
end
