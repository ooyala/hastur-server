require "hastur-server/util"
require "hastur-server/message"
require "hastur-server/cassandra/schema"
require "cassandra"
require "cassandra/1.0"

module Hastur
  module Service
    class CassandraSink
      attr_reader :data_uri, :ack_uri

      # retry up to 60 times with 1 second sleeps, so we wait a whole minute before
      # discarding a message due to a cassandra node/cluster going down
      RECONNECT_ATTEMPTS = 60
      RECONNECT_SLEEP = 1.0

      #
      # @param [Hash{Symbol => Object}] opts
      # @option [ZMQ::Context] :ctx
      # @option [String] :data_uri ZMQ uri to subscribe on for data
      # @option [String] :ack_uri ZMQ uri to write acks to
      # @option [String] :keyspace Cassandra keyspace to write to
      # @option [Array<String>] :cassandra list of cassandra servers
      # @option [Fixnum] :socktype ZMQ socket type default ZMQ::PULL (only PULL / SUB really make sense)
      #
      # @example
      #   Hastur::Service::CoreSink.supervise_as(:sink,
      #     :data_uri  => 'tcp://127.0.0.1:8128',
      #     :ack_uri   => 'tcp://127.0.0.1:8127',
      #     :keyspace  => 'Hastur',
      #     :cassandra => [ '127.0.0.1:9160' ],
      #     :socktype  => ZMQ::SUB,
      #   )
      #
      def initialize(opts={})
        @ctx = opts[:ctx] || ZMQ::Context.new
        @data_uri = opts[:data_uri]
        @ack_uri = opts[:ack_uri]
        @socktype = opts[:socktype] || ZMQ::PULL
        @logger = opts[:logger] || Termite::Logger.new

        @sockopts = { :hwm => opts[:hwm] || 1, :linger => opts[:linger] || 1_000 }

        [:data_uri, :ack_uri, :keyspace, :cassandra].each do |p|
          raise "Named parameter :#{p} is required." unless opts[p]
        end

        @keyspace = opts[:keyspace]
        @cassandra_servers = opts[:cassandra].flatten

        @running = false
      end

      #
      # Connect to the Cassandra cluster, implementing manual server rotation.
      #
      # While we've been using them all along, in manual testing, arrays of servers
      # doesn't seem to work with thrift_client, so here's a quick & dirty workaround.
      # TODO: move this to a helper
      # TODO: Get away from thrift_client
      #
      def connect_to_cassandra
        if @client
          @client.disconnect! rescue nil
        end
        @client = nil

        @cassandra_servers.each do |server|
          begin
            c = ::Cassandra.new @keyspace, server
            ring = c.ring
            @client = c
            break
          rescue ThriftClient::NoServersAvailable
            @logger.warn "Cassandra server #{server} seems to be unavailable."
          end
        end

        if @client
          @logger.info "Connected to Cassandra: #{@client.inspect}"
        else
          raise "Could not connect to any server in server list: #@cassandra_servers"
        end

        @client
      end

      #
      # Only valid for ZMQ::SUB sockets: add a subscription to the socket
      # @param [String] subscription message prefix to subscribe to
      #
      def subscribe(subscription)
        raise "subscribe is only valid on ZMQ::SUB sockets" unless @socktype == ZMQ::SUB
        @data_socket.setsockopt ZMQ::SUBSCRIBE, subscription
      end

      #
      # Connect to Cassandra and ZeroMQ sockets, register poller.
      #
      def setup
        connect_to_cassandra

        @data_socket = Hastur::Util.connect_socket @ctx, @socktype, @data_uri, @sockopts
        @ack_socket  = Hastur::Util.connect_socket @ctx, ZMQ::PUSH, @ack_uri,  @sockopts

        @poller = ZMQ::Poller.new
        @poller.register_readable @data_socket
      end

      #
      # Enter the read/write loop reading from ZMQ, writing to Cassandra. Retries cassandra
      # connections manually if it fails, since the gem doesn't seem to work as expected.
      # @return [Boolean] final value of the run flag
      #
      def run
        @running = true

        while @running do
          message = Hastur::Message.recv(@data_socket)
          envelope = message.envelope
          uuid = message.envelope.from
          Hastur::Cassandra.insert(@client, message.payload, envelope.type_symbol.to_s, :uuid => uuid)
          envelope.to_ack.send(@ack_socket) if envelope.ack?
        end

      rescue Hastur::ZMQError => e
        @logger.error "Error reading from ZeroMQ socket.", { :exception => e, :backtrace => e.backtrace }
      rescue Exception => e
        @logger.warn "unhandled exception! Dropped message: #{message}"
        Hastur::Util.log_exception e, @logger
        raise e
      end

      #
      # Return true/false of the run flag.
      #
      def running?
        @running
      end

      #
      # Set the run flag to false and let the loop exit gracefully.
      #
      def stop
        @running = false
      end

      #
      # Close ZeroMQ and Cassandra sockets.
      #
      def shutdown
        @data_socket.close
        @ack_socket.close
        @client.disconnect! rescue nil
      end
    end
  end
end
