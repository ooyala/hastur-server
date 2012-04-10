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

        @done = true

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
          message = Hastur::Util.read_strings(@socket)
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
        marsh = Marshal.dump message
        tuuid = @queue.push marsh
        # Add tuuid as the first part, and then pass the message on the inproc
        message.unshift tuuid
        Hastur::Util.send_strings(@ssock, message)
        Hastur::Util.send_strings(@socket, [tuuid, "OK"])
      end

      #
      # This is the method to get an element from the queue
      # On the backend, it will take the first message off the inproc, and forward it to the worker
      #
      def method_get(message)
        if @done
          message = Hastur::Util.read_strings(@rsock)
          @message = message
          @done = false
        end
        # Send along the message with the tuuid as the first part
        Hastur::Util.send_strings(@socket, @message)
      end

      #
      # This is the method that is called to say you are done processing a message,
      # so that it can be deleted from the queue
      #
      def method_done(message)
        tuuid = message.shift
        @queue.remove(tuuid)
        @done = true
        Hastur::Util.send_strings(@socket, [tuuid, "OK"])
      end

      #
      # Figure out with type of request is being made,
      # and then call the approrpiate function, after stripping it from the message.
      #
      def pick_method(message)
        case method = message.shift
        when "submit"
          method_submit message
        when "get"
          method_get message
        when "done"
          method_done message
        else
          err = "Invalid Request. Please have the first part of your message be one of {submit, get, done}"
          Hastur::Util.send_strings(@socket, [err])
          STDERR.puts "Request made with invalid method: #{method}"
        end
      end

    end
  end
end
