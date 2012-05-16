require "hastur-server/util"
require "hastur-server/message"
require "hastur-server/cassandra/schema"
require "cassandra"
require "cassandra/1.0"

module Hastur
  module Service
    class CassandraSink
      attr_reader :data_uri, :ack_uri

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
        @logger = Termite::Logger.new

        sopt = { :hwm => opts[:hwm] || 1, :linger => opts[:linger] || 10 }

        [:data_uri, :ack_uri, :keyspace, :cassandra].each do |p|
          raise "Named parameter :#{p} is required." unless opts[p]
        end

        @client = ::Cassandra.new(opts[:keyspace], opts[:cassandra].flatten)

        @data_socket = Hastur::Util.connect_socket @ctx, @socktype, @data_uri, sopt
        @ack_socket  = Hastur::Util.connect_socket @ctx, ZMQ::PUSH, @ack_uri,  sopt

        @running = false
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
      # Enter the read/write loop.
      #
      def run
        @running = true
        while @running do
          begin
            message = Hastur::Message.recv(@data_socket)
            envelope = message.envelope
            uuid = message.envelope.from
            Hastur::Cassandra.insert(@client, message.payload, envelope.type_symbol.to_s, :uuid => uuid)
            envelope.to_ack.send(@ack_socket) if envelope.ack?
          rescue Hastur::ZMQError => e
            @logger.error "Error reading from ZeroMQ socket.", { :exception => e }
          rescue Exception => e
            @logger.error e.to_s, { :exception => e }
          end
        end
      end

      def stop
        @running = false
        @data_socket.close
        @ack_socket.close
      end
    end
  end
end
