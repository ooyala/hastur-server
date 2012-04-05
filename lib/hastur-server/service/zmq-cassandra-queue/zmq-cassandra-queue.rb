require "ffi-rzmq"
require "celluloid"

require_relative "zmq-cassandra-actors"

module Hastur
  module Service
    module ZmqCassandra
      class Queue
        include Celluloid

        attr_reader :incoming_uri, :outgoing_uri
        #
        # Sets up a persistent cassandra-backed zmq queue.
        # @param [String] The queueUUID for the queue to use
        # @param [Hash{Symbol => String}] opts
        # @option [String] :router_uri default tcp://*:8126
        # @option [String] :incoming_uri default tcp://*:8127
        # @option [String] :outgoing_uri default tcp://*:8128
        #
        def initialize(qid, incoming_uri = "tcp://*:8187", outgoing_uri = "tcp://*:8188")
          @ctx = ::ZMQ::Context.new

          @incoming_uri = incoming_uri
          @outgoing_uri = outgoing_uri

          @incoming_socket = @ctx.socket(::ZMQ::PUSH)
          @outgoing_socket = @ctx.socket(::ZMQ::PULL)

          setsockopts(@incoming_socket)
          setsockopts(@outgoing_socket)

          bind(@incoming_socket, @incoming_uri)
          bind(@outgoing_socket, @outgoing_uri)
          @producer = Producer.new(qid, @incoming_socket)
          @consumer = Consumer.new(qid, @outgoing_socket)
        end

        def stop
          @producer.stop
          @consumer.stop
          @ctx.terminate
        end

        private

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
end
