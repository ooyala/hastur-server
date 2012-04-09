require "ffi-rzmq"
require "cassandra-queue"

module Hastur
  module Service
    class Queue

      INPROC_URI = "inproc://queue_inproc"

      attr_reader :uri
      #
      # Sets up a persistent cassandra-backed zmq queue.
      # @param [String] The queueUUID for the queue to use
      # @param [Hash{Symbol => String}] opts
      # @option [String] :router_uri default tcp://*:8126
      # @option [String] :incoming_uri required
      # @option [String] :outgoing_uri required
      #
      def initialize(qid, opts = {})
        # Make sure REQUIRED_OPTS are defined
        raise "URIs not defined in opts" unless opts.has_key? :uri

        @qid = qid
        @uri = opt[:uri]
        @ctx = opt[:ctx] || ::ZMQ::Context.new

        @queue = CassandraQueue::Queue.get_queue(@qid)

        @socket = @ctx.socket(::ZMQ::ROUTER)
        Hastur::Util.setsockopts(@socket)
        Hastur::Util.bind(@socket, @uri)

        @ssock = @ctx.socket(ZMQ::PAIR)
        @rsock = @ctx.socket(ZMQ::PAIR)
        Hastur::Util.setsockopts([@rsock, @ssock], :hwm => 0)
        Hastur::Util.connect(@ssock, INPROC_URI)
        Hastur::Util.bind(@rsock, INPROC_URI)
      end

      def run
        @running = true
        while @running
          @socket.recv_strings message=[]
          pick_method message
        end
      end

      def stop
        @running = false
        @socket.close
        @ctx.terminate
      end

      private

      def method_submit

      end

      def method_get

      end

      def method_done

      end

      def pick_method message
        case message[0]
        when "submit"
          method_submit message
        when "get"
          method_get message
        when "done"
          method_done message
        end
      end

    end
  end
end
