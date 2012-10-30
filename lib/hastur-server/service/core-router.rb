require "ffi-rzmq"
require "hastur/api"
require "hastur-server/message"
require "hastur-server/router"

module Hastur
  module Service
    class CoreRouter
      attr_reader :router_uri, :firehose_uri, :return_uri

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

        # agent UUID to ZMQ envelope ID mapping cache
        @agents = {}
        @noop_type_id = Hastur::Message.symbol_to_type_id(:noop) # cache the value

        @router_uri   = opts[:router_uri]
        @firehose_uri = opts[:firehose_uri]
        @return_uri   = opts[:return_uri]

        @last_counter_dump = Time.now

        @counters = {
          'poll.count'         => 0,
          'messages.forwarded' => 0,
          'firehose.bytes'     => 0,
          'messages.returned'  => 0,
          'return.bytes'       => 0,
        }

        @running = false
      end

      def setup
        # buffer up to 10_000 messages, wait up to 10 seconds on shutdown for them to flush to the sink
        sopt = { :hwm => 1_000, :linger => 10_000 }
        @router_socket   = Hastur::Util.bind_socket @ctx, ZMQ::ROUTER, @router_uri,   sopt
        @firehose_socket = Hastur::Util.bind_socket @ctx, ZMQ::PUB,    @firehose_uri, sopt
        @return_socket   = Hastur::Util.bind_socket @ctx, ZMQ::PULL,   @return_uri,   sopt

        @poller = ZMQ::Poller.new
        @poller.register_readable @router_socket
        @poller.register_readable @return_socket
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
      # Return true/false of the run flag.
      #
      def running?
        @running
      end

      #
      # signal the end of the poll loop
      #
      def stop
        @running = false
      end

      def shutdown
        @router_socket.close
        @return_socket.close

        # router flushes to firehose, so give it a little time to do work since
        # closing pub/sub sockets will destroy pending messages
        sleep 1.0
        @firehose_socket.close
      end

      private

      #
      # Poll the sockets, forward data.
      #
      def poll
        rc = @poller.poll 100 # milliseconds!
        @counters['poll.count'] += 1

        if ::ZMQ::Util.resultcode_ok? rc
          @poller.readables.each do |r|
            if r == @router_socket
              forward_router_to_firehose
            elsif r == @return_socket
              forward_return_to_router
            end
          end
        else
          Hastur.event "hastur.router.zmq.error", ZMQ::Util.error_string
        end


        # send out counters roughly every 30 seconds, more often if things are busy
        now = Time.now
        if now - @last_counter_dump > 30
          @last_counter_dump = now
          @counters.keys.each do |name|
            Hastur.counter "hastur.router.#{name}", @counters[name]
            @counters[name] = 0
          end
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
          Hastur.event "hastur.router.zmq.error", ZMQ::Util.error_string
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
          Hastur.event "hastur.router.zmq.error", ZMQ::Util.error_string
          false
        end
      end

      #
      # record errors in regular logs and in Hastur events
      #
      # @param [Array<ZMQ::Message>] message
      # @param [Exception] e
      #
      def record_message_exception(message, e)
        raw = message.map { |m| m.copy_out_string rescue 'FAIL' }
        edata = { :exception => e.inspect, :backtrace => e.backtrace, :raw_messages => raw }

        @logger.warn "Exception while forwarding message: #{e}", edata
        Hastur.event "hastur.router.exception", e.to_s, 'hastur-admin', MultiJson.dump(edata)
      end

      #
      # count up the number of bytes in a multi-part zeromq message
      #
      # @param [Array<ZMQ::Message>] message
      #
      def message_bytes(message)
        message.reduce(0) do |sum, msg|
          sum + (msg.size rescue 0)
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
            envelope = Hastur::Envelope.parse message[-2].copy_out_string
            if envelope.type_id == @noop_type_id
              # noops are used to maintain the reverse path for acks from core -> agent
              @agents[envelope.from] = message[0].copy_out_string
            else
              # forward the message, sans the ZMQ envelope, rely on sender to close those messages
              send_to @firehose_socket, message.slice!(-2, 2)
              @counters['messages.forwarded'] += 1
              @counters['firehose.bytes'] += 1
            end
          rescue Exception => e
            record_message_exception message, e
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
        begin
          message = read_from @return_socket

          if message
            envelope = Hastur::Envelope.parse message[-2].copy_out_string
            agent_id = @agents[envelope.from]
            message.unshift ::ZMQ::Message.new(agent_id)
            send_to @router_socket, message
            @counters['messages.returned'] += 1
            @counters['return.bytes'] += 1
          end
        rescue
          record_message_exception message, e
        ensure
          message.each do |m| m.close end
        end
      end
    end
  end
end
