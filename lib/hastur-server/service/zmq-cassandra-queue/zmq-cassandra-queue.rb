require "ffi-rzmq"
require "celluloid"

require_relative "zmq-cassandra-actors"

module Hastur
  module Service
    module ZmqCassandra
      class Queue
        include Celluloid

        REQUIRED_OPTS = [:incoming_uri, :outgoing_uri]
        attr_reader :incoming_uri, :outgoing_uri
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
          raise "URIs not defined in opts" unless opts.keys & REQUIRED_OPTS == REQUIRED_OPTS

          @incoming_uri = opt[:incoming_uri]
          @outgoing_uri = opt[:outgoing_uri]
          @ctx = opt[:ctx] || ::ZMQ::Context.new
        end

        def run
          @running = true

          @consumer = Consumer.new(qid, {:ctx => @ctx, :uri => @outgoing_uri})
          @producer = Producer.new(qid, {:ctx => @ctx, :uri => @incoming_uri, :consumer => @consumer})

          @producer.supervise_as :producer
          @consumer.supervise_as :consumer
          sleep
        end

        def stop
          @running = false
          # @producer.stop
          # @consumer.stop
          @ctx.terminate
        end

        private
      end
    end
  end
end
