require "ffi-rzmq"
require "hastur-server/message"
require "hastur-server/router"

module Hastur
  module Service
    class CoreRouter
      #
      # Create a new core router.
      # @param [String] uuid the core router's UUID (36 byte hyphenated string)
      # @param [Hash{Symbol => ZMQ::Socket}] opts
      # @option [String] :router_uri Required, the ZMQ URI of the router socket
      # @option [String] :firehose_uri Required, the ZMQ URI of the firehose PUB socket
      # @option [String] :return_uri Required, the ZMQ URI of the return/ack PULL socket
      # @option [ZMQ::Context] :ctx
      # @option [Logger] :logger
      #
      def initialize(uuid, opts={})
        @ctx = opts[:ctx] || ZMQ::Context.new
        @logger = opts[:logger] || Termite::Logger.new

        sopt = { :hwm => 1_000, :linger => 10 }

        @router_socket   = Hastur::Util.bind_socket @ctx, ZMQ::ROUTER, opts[:router_uri],   sopt
        @firehose_socket = Hastur::Util.bind_socket @ctx, ZMQ::PUB,    opts[:firehose_uri], sopt
        @return_socket   = Hastur::Util.bind_socket @ctx, ZMQ::PULL,   opts[:return_uri],   sopt

        @poller = ZMQ::Poller.new
        @poller.register_readable @router_socket
        @poller.register_readable @return_socket

        # agent UUID to ZMQ envelope ID mapping cache
        @agents = {}
        @noop_type_id = Hastur::Message.symbol_to_type_id(:noop) # cache the value

        @running = false
      end

      #
      # start the poll loop
      #
      def run
        @running = true
        while @running
          poll
        end
      end

      #
      # signal the end of the poll loop
      #
      def stop
        @running = false
      end

      private

      #
      # Poll the sockets, forward data.
      #
      def poll
        rc = @poller.poll 1
        if ::ZMQ::Util.resultcode_ok? rc
          @poller.readables.each do |r|
            if r == @router_socket
              forward_router_to_firehose
            elsif r == @return_socket
              forward_return_to_router
            end
          end
        else
          @logger.error ZMQ::Util.error_string
        end
      end

      #
      # read a message from the zmq socket, check for errors, report on the error
      # socket if something goes wrong
      # @param [ZMQ::Socket] socket
      # @return [Array<ZMQ::Message>, false] a list of messages on success, false on failure
      #
      def read_from(socket)
        message = []
        rc = socket.recvmsgs message
        if ::ZMQ::Util.resultcode_ok? rc
          message
        else
          @logger.error ZMQ::Util.error_string
          false
        end
      end

      #
      # send a message on a zmq socket, check for errors
      # @param [ZMQ::Socket] socket
      # @param [Array<ZMQ::Message>] message
      # @return [Boolean]
      #
      def send_to(socket, message)
        rc = socket.sendmsgs message
        if ::ZMQ::Util.resultcode_ok? rc
          true
        else
          @logger.error ZMQ::Util.error_string
          false
        end
      end

      #
      # read from the router socket, write to the firehose
      #
      def forward_router_to_firehose
        message = read_from @router_socket

        if message
          begin
            # cache the ZMQ envelope to route messages back to agents
            content = message[-2].copy_out_string
            envelope = Hastur::Envelope.parse content
            if envelope.type_id == @noop_type_id
              # noops are used to maintain the reverse path for acks from core -> agent
              @agents[envelope.from] = message[0].copy_out_string
            else
              # forward the message, sans the ZMQ envelope
              message.shift
              send_to @firehose_socket, message
            end
          rescue Exception => e
            payload = message[-1].copy_out_string rescue "<error>"
            @logger.warn("Exception while forwarding message: #{e}", {
              :backtrace    => e.backtrace,
              :envelope     => envelope.to_hash,
              :raw_envelope => content.inspect,
              :payload      => payload,
            })
          ensure
            # close all of the zmq messages or we might leak C memory
            message.each do |m| m.close end
          end
        end
      end

      #
      # read from the return socket, insert the socket ID, write to the router socket
      #
      def forward_return_to_router
        message = read_from @return_socket

        if message
          envelope = Hastur::Envelope.parse message[-2].copy_out_string
          agent_id = @agents[envelope.from]
          message.unshift ::ZMQ::Message.new(agent_id)
          send_to @router_socket, message
        end
      end
    end
  end
end
