require "ffi-rzmq"
require "cassandra-queue"
require_relative "../util"

module Hastur
  module Service
    class WorkQueue

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
        @uri = opts[:uri]
        @ctx = opts[:ctx] || ::ZMQ::Context.new

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
        Hastur::Util.bind(@rsock, INPROC_URI)
        Hastur::Util.connect(@ssock, INPROC_URI)

        @poller = ZMQ::Poller.new
        @poller.register(@rsock, ZMQ::POLLIN)

        # TODO(jbhat): Start up background thread that replays old work into the inproc
        _start_replay_thread
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
      def method_submit(header, message)
        marsh = Marshal.dump message
        tuuid = @queue.push(marsh).to_s
        # Add tuuid as the first part, and then pass the message on the inproc
        message.unshift tuuid
        Hastur::Util.send_strings(@ssock, message)
        Hastur::Util.send_strings(@socket, header.concat([tuuid, "OK"]))
      end

      #
      # This is the method to get an element from the queue
      # On the backend, it will take the first message off the inproc, and forward it to the worker
      #
      def method_get(header, _)
        # Get a message off the inproc if always delivering a new message,
        # or if the previous message has been acked
        rc = @poller.poll_nonblock
        if ::ZMQ::Util.resultcode_ok? rc
          if @poller.readables.size > 0
              @message = Hastur::Util.read_strings(@rsock)
          else
              @message = ["No work in queue!"]
          end
        else
          @message = ["Unable to poll the queue's internal socket: #{::ZMQ::Util.error_string}"]
        end
        # Send along the message with the tuuid as the first part
        Hastur::Util.send_strings(@socket, header.concat(@message))
      end

      #
      # This is the method that is called to say you are done processing a message,
      # so that it can be deleted from the queue
      #
      def method_done(header, message)
        tuuid = message.shift
        @queue.remove(tuuid)
        Hastur::Util.send_strings(@socket, header.concat([tuuid, "OK"]))
      end

      #
      # Figure out with type of request is being made,
      # and then call the approrpiate function, after stripping it from the message.
      #
      def pick_method(message)
        header = message[0..1]
        my_message = message[2..-1]
        case method = my_message.shift
        when "submit"
          method_submit header, my_message
        when "get"
          method_get header, my_message
        when "done"
          method_done header, my_message
        else
          err = "Invalid Request. Please have the first part of your message be one of {submit, get, done}"
          Hastur::Util.send_strings(@socket, header << err)
          STDERR.puts "Request made with invalid method: #{method}"
        end
      end

      private

      #
      # Starts a replay thread that reads unprocessed work from C* and enqueues
      # the messages into this queue
      #
      # TODO: Need to worry about cases where there are too many messages in C* to
      #       completely store in memory.
      #
      def _start_replay_thread
        @replay_thrd = Thread.start do

        end
      end
    end
  end
end
