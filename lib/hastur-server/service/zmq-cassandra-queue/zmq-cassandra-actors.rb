# Temporary hack to require cassandra-queue from source
$LOAD_PATH << "#{ENV['REPO_CODE_ROOT']}/cassandra-queue/lib"
require "cassandra-queue"
require "ffi-rzmq"
require "celluloid"

module Hastur
  module Service
    module ZmqCassandra

      def self.setsockopts(sock)
        rc = sock.setsockopt(::ZMQ::LINGER, -1)
        raise "Error setting ZMQ::LINGER: #{::ZMQ::Util.error_string}" unless rc > -1
        rc = sock.setsockopt(::ZMQ::HWM, 1)
        raise "Error setting ZMQ::HWM: #{::ZMQ::Util.error_string}" unless rc > -1
      end

      def self.bind(sock, uri)
        rc = sock.bind(uri)
        raise "Could not bind socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
      end

      REQUIRED_PRODUCER_OPTS = [:ctx, :incoming_uri, :consumer]
      class Producer
        include Celluloid

        def initialize(qid, opts = {})
          raise "URIs not defined in opts" unless opts.keys & REQUIRED_PRODUCER_OPTS == REQUIRED_PRODUCER_OPTS

          @qid = qid
          @ctx = opts[:ctx]
          @incoming_uri = opts[:incoming_uri]
          @consumer = opts[:consumer]
          @queue = CassandraQueue::Queue.get_queue(@qid)

          @incoming_socket = @ctx.socket(::ZMQ::PULL)
          Hastur::Service::ZmqCassandra.setsockopts(@incoming_socket)
          Hastur::Service::ZmqCassandra.bind(@incoming_socket, @incoming_uri)

          @poller = ::ZMQ::Poller.new
          @poller.add_readable @incoming_socket
        end

        def run
          @running = true
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
              if r == @incoming_socket
                # Push the message to the queue, and send it to the consumer
                rc = @incoming_socket.recv_strings message=[]
                marsh = Marshall.dump message
                tuuid = @queue.push marsh
                @consumer.push_message! tuuid, message
              else
                send_error ::ZMQ::Util.error_string
              end
            end
          else
            send_error ::ZMQ::Util.error_string
          end
        end

      end

      REQUIRED_CONSUMER_OPTS = [:ctx, :outgoing_uri]
      class Consumer
        include Celluloid

        def initialize(qid, opts = {})
          raise "URIs not defined in opts" unless opts.keys & REQUIRED_CONSUMER_OPTS == REQUIRED_CONSUMER_OPTS

          @qid = qid
          @ctx = opts[:ctx]
          @outgoing_uri = opts[:outgoing_uri]
          @poller = ::ZMQ::Poller.new
          @queue = CassandraQueue::Queue.get_queue(@qid)

          @outgoing_socket = @ctx.socket(::ZMQ::PUSH)

          Hastur::Service::ZmqCassandra.setsockopts(@outgoing_socket)
          Hastur::Service::ZmqCassandra.bind(@outgoing_socket, @outgoing_uri)

        end

        def stop
          @outgoing_socket.close
        end

        def push_message(tuuid, message)
          # Write to outbound socket, then delete from cassandra
          @outbound_socket.send message
          delete_message(tuuid)
        end

        def delete_message(tuuid)
          # Wait for ack of delivery?
          @queue.remove tuuid
        end
      end

    end
  end
end