# Temporary hack to require cassandra-queue from source
$LOAD_PATH << "#{ENV['REPO_CODE_ROOT']}/cassandra-queue/lib"
require "cassandra-queue"
require "ffi-rzmq"
require "celluloid"

module Hastur
  module Service
    module ZmqCassandra

      class Producer
        include Celluloid

        def initialize(qid, incoming_socket)
          @qid = qid
          @incoming_socket = incoming_socket
          @poller = ::ZMQ::Poller.new
          @queue = CassandraQueue::Queue.get_queue(@qid)

          # Connect to inproc socket with consumer

        end

        def run
          while @running
            poll
          end
        end

        def stop
          @running = false
          @incoming_socket.close
        end

        private

        # Poll for incoming messages.  For each message
        def poll
          rc = @poller.poll 1
          if ::ZMQ::Util.resultcode_ok? rc
            @poller.readables.each do |r|
              # Push the message to the queue, and send it to the consumer over the inproc

            end
          else
            send_error ::ZMQ::Util.error_string
          end
        end

      end

      class Consumer
        include Celluloid

        def initialize(qid, outgoing_socket)
          @qid = qid
          @outgoing_socket = outgoing_socket
          @poller = ::ZMQ::Poller.new
          @queue = CassandraQueue::Queue.get_queue(@qid)

          # Bind to inproc socket for producer

        end

        def run
          while @running
            poll
          end
        end

        def stop
          @running = false
          @outgoing_socket.close
        end

        private

        def poll
          rc = @poller.poll 1
          if ::ZMQ::Util.resultcode_ok? rc
            @poller.readables.each do |r|
              # Send the messaage out on the outgoing socket, and then delete it from the queue

            end
          else
            send_error ::ZMQ::Util.error_string
          end
        end

      end

    end
  end
end