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
      # @option [String] :uri required
      #
      def initialize(qid, opts = {})
        # Make sure required options are defined
        raise "URIs not defined in opts" unless opts.has_key? :uri

        @qid = qid
        @uri = opt[:uri]
        @ctx = opt[:ctx] || ::ZMQ::Context.new

        # Create the queue client for the cassandra-backed queue
        @queue = CassandraQueue::Queue.get_queue(@qid)

        # Setup outbound communication socket
        @socket = @ctx.socket(::ZMQ::ROUTER)
        Hastur::Util.setsockopts(@socket)
        Hastur::Util.bind(@socket, @uri)

        # Setup inproc socket
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

      #
      # This is the method that is called to submit something to the queue
      #
      def method_submit(message)

      end

      #
      # This is the method to get an element from the queue
      #
      def method_get(message)

      end

      #
      # This is the method that is called to say you are done processing a message,
      # so that it can be deleted from the queue
      #
      def method_done(message)

      end

      def pick_method(message)
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
