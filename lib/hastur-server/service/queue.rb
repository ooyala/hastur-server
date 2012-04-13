require "ffi-rzmq"
require "cassandra-queue"
require "timeout"
require_relative "../util"

module Hastur
  module Service
    class Queue

      TIMEOUT = 0.1

      attr_reader :incoming_uri, :outgoing_uri
      #
      # Sets up a persistent cassandra-backed zmq queue.
      # @param [String] The queueUUID for the queue to use
      # @param [Hash{Symbol => String}] opts
      # @option [String] :incoming_uri required
      # @option [String] :outgoing_uri required
      #
      def initialize(qid, opts = {})
        # Make sure required options are defined
        raise "URIs not defined in opts" unless (opts.has_key?(:incoming_uri) && opts.has_key?(:outgoing_uri))

        # Create the queue client for the cassandra-backed queue
        @qid = qid
        @queue = opts[:queue] || CassandraQueue::Queue.get_queue(@qid)

        @ctx = opts[:ctx] || ::ZMQ::Context.new

        @incoming_uri = opts[:incoming_uri]
        @incoming_socket = @ctx.socket(::ZMQ::PULL)
        Hastur::Util.setsockopts(@incoming_socket)
        Hastur::Util.bind(@incoming_socket, @incoming_uri)

        @outgoing_uri = opts[:outgoing_uri]
        @outgoing_socket = @ctx.socket(::ZMQ::PUSH)
        Hastur::Util.setsockopts(@outgoing_socket)
        Hastur::Util.bind(@outgoing_socket, @outgoing_uri)

        @poller = ZMQ::Poller.new
        @poller.register_readable @incoming_socket

        # Setup inproc socket
        @ssock = @ctx.socket(ZMQ::PAIR)
        @rsock = @ctx.socket(ZMQ::PAIR)
        Hastur::Util.setsockopts([@rsock, @ssock], :hwm => 0)
        Hastur::Util.bind(@rsock, INPROC_URI)
        Hastur::Util.connect(@ssock, INPROC_URI)

        @running = true

        # TODO: Move this to the other thread (replay on startup)
        @undelivered_messages = !_replay_queue

      end

      def run
        while @running
          poll
        end
      end

      def stop
        @running = false
        @incoming_socket.close
        @outgoing_socket.close
        @ctx.terminate
      end

      private

      def poll
        rc = @poller.poll 1
        if ::ZMQ::Util.resultcode_ok? rc
          @poller.readables.each do |r|
            if r == @incoming_socket
              message = Hastur::Util.read_strings(@incoming_socket)
              method_submit message
            end
          end
        else
          send_error ::ZMQ::Util.error_string
        end
      end

      #
      # This is the method that is called to submit something to the queue
      # it will then call the method to forward the message to the push socket, and then delete it
      def method_submit(message)
        marsh = Marshal.dump message
        tuuid = @queue.push(marsh).to_s
        # TODO: move this work to another thread, and deliver the message to it via internal queue (inproc)
        @undelivered_messages ? _replay_queue : method_send(tuuid, message)
      end

      #
      # This is the method that tries to push the message out the outgoing socket
      # TODO: This should run in its own process and read off of an internal queue
      #
      def method_send(tuuid, message, replay_tries = 1)
        begin
          rv = Timeout::timeout(TIMEOUT) { Hastur::Util.send_strings @outgoing_socket, message }
          if rv
            method_remove(tuuid)
            true
          else
            _replay_queue(replay_tries)
            false
          end
        rescue Timeout::Error
          _replay_queue(replay_tries)
          false
        end
      end

      #
      # This is the method that is called to say you are done processing a message,
      # so that it can be deleted from the queue
      #
      def method_remove(tuuid)
        @queue.remove(tuuid)
      end

      private

      #
      # Get all the messages out of cassandra, and send them all out over the outgoing socket
      #
      # TODO: Need to worry about cases where there are too many messages in C* to
      #       completely store in memory.
      # TODO: Move this into the second thread and use if we get sequence number mismatch
      #
      def _replay_queue(tries = 1)
        @undelivered_messages = true
        return false if tries == 0
        messages = @queue.list(true)
        messages.each do |tuuid, marsh|
          message = Marshal.load marsh
          rv = method_send(tuuid, message, tries - 1)
          unless rv
            return false
          end
        end
        @undelivered_messages = false
        true
      end
    end
  end
end
