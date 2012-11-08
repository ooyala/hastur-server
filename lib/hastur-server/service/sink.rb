require "ffi-rzmq"
require "hastur/api"
require "hastur-server/util"
require "hastur-server/message"
require "hastur-server/router"
require "hastur-server/cassandra/schema"
require "cassandra"
require "cassandra/1.0"

module Hastur
  module Service
    class Sink
      attr_reader :router_uri, :return_uri, :cassandra_servers, :keyspace

      #
      # Create a new core router.
      # @param [String] uuid the core router's UUID (36 byte hyphenated string)
      # @param [Hash{Symbol => ZMQ::Socket}] opts
      # @option [Logger] :logger
      # @option [ZMQ::Context] :ctx
      # @option [String] :router_uri Required, the ZMQ URI of the router socket
      # @option [String] :return_uri Required, the ZMQ URI of the return/ack PULL socket
      # @option [String] :keyspace Cassandra keyspace to write to
      # @option [Array<String>] :cassandra list of cassandra servers
      #
      # @example
      #   Hastur::Service::Sink(uuid,
      #     :router_uri => 'tcp://127.0.0.1:8126',
      #     :return_uri => 'tcp://127.0.0.1:8127',
      #     :keyspace   => 'hastur',
      #     :cassandra  => [ '127.0.0.1:9160' ],
      #   )
      #

      def initialize(uuid, opts={})
        [:router_uri, :return_uri, :keyspace, :cassandra].each do |p|
          raise "Named parameter :#{p} is required." unless opts[p]
        end

        @ctx               = opts[:ctx]    || ZMQ::Context.new
        @logger            = opts[:logger] || Termite::Logger.new
        @router_uri        = opts[:router_uri]
        @return_uri        = opts[:return_uri]
        @keyspace          = opts[:keyspace]
        @cassandra_servers = [opts[:cassandra]].flatten

        # buffer only 1 message, wait up to 2 seconds on shutdown for them to flush to cassandra
        @sockopts = { :hwm => opts[:hwm] || 1, :linger => opts[:linger] || 2_000 }

        # agent UUID to ZMQ envelope ID mapping cache
        @agents = {}
        @noop_type_id = Hastur::Message.symbol_to_type_id(:noop) # cache the value

        @last_counter_dump = Time.now
        @counters = {
          'poll.count'         => 0,
          'messages.forwarded' => 0,
          'messages.acked'     => 0,
          'messages.returned'  => 0,
        }

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
        if @cass_client
          @cass_client.disconnect! rescue nil
        end
        @cass_client = nil

        @cassandra_servers.each do |server|
          begin
            c = ::Cassandra.new @keyspace, server
            c.ring # will raise an exception if the connection is no good
            @cass_client = c
            break
          rescue ThriftClient::NoServersAvailable
            @logger.warn "Cassandra server #{server} seems to be unavailable."
          end
        end

        if @cass_client
          @logger.info "Connected to Cassandra: #{@cass_client.inspect}"
        else
          raise "Could not connect to any server in server list: #@cassandra_servers"
        end

        @cass_client
      end

      #
      # Connect to Cassandra and ZeroMQ sockets, register poller.
      #
      def setup
        connect_to_cassandra

        @router_socket = Hastur::Util.bind_socket @ctx, ZMQ::ROUTER, @router_uri, @sockopts
        @return_socket = Hastur::Util.bind_socket @ctx, ZMQ::PULL,   @return_uri, @sockopts

        @poller = ZMQ::Poller.new
        @poller.register_readable @router_socket
        @poller.register_readable @return_socket
      end

      #
      # start the poll loop
      #
      def run
        @running = true
        while @running
          poll
        end
      end

      #
      # Return true/false of the run flag.
      #
      def running?
        @running
      end

      #
      # signal the end of the poll loop
      #
      def stop
        @running = false
      end

      #
      # Close ZeroMQ and Cassandra sockets.
      #
      def shutdown
        @router_socket.close
        @return_socket.close
        @cass_client.disconnect! rescue nil
      end

      private

      #
      # Poll the sockets, forward data.
      #
      def poll
        rc = @poller.poll 100 # milliseconds!
        @counters['poll.count'] += 1

        if ::ZMQ::Util.resultcode_ok? rc
          @poller.readables.each do |r|
            if r == @router_socket
              forward_router_to_cassandra
            elsif r == @return_socket
              forward_return_to_router
            end
          end
        else
          Hastur.event "hastur.router.zmq.error", ZMQ::Util.error_string
        end


        # send out counters roughly every 30 seconds, more often if things are busy
        now = Time.now
        if now - @last_counter_dump > 30
          @last_counter_dump = now
          @counters.keys.each do |name|
            Hastur.counter "hastur.router.#{name}", @counters[name]
            @counters[name] = 0
          end
        end
      end

      #
      # count up the number of bytes in a multi-part zeromq message
      #
      # @param [Array<ZMQ::Message>] message
      #
      def message_bytes(message)
        message.reduce(0) do |sum, msg|
          sum + (msg.size rescue 0)
        end
      end

      #
      # read from the router socket, write to cassandra
      #
      def forward_router_to_cassandra
        start = Time.now
        message = Hastur::Message.recv(@router_socket)
        envelope = message.envelope

        # cache the ZMQ envelope to route messages back to agents
        if envelope.type_id == @noop_type_id
          # noops are used to maintain the reverse path for acks from core -> agent
          @agents[envelope.from] = message.zmq_parts[0]
        else
          uuid = message.envelope.from
          Hastur::Cassandra.insert(@cass_client, message.payload, envelope.type_symbol.to_s, :uuid => uuid)
          @counters['messages.acked'] += 1
          envelope.to_ack.send(@return_socket) if envelope.ack?
          @counters['messages.forwarded'] += 1
        end
      rescue Hastur::ZMQError => e
        @logger.error "Error reading from ZeroMQ socket.", { :exception => e, :backtrace => e.backtrace }
      rescue CassandraThrift::Cassandra::Client::TransportException => e
        failed = Time.now
        puts "Failed after #{failed.to_f - start.to_f} seconds."
        raise e
      rescue Exception => e
        edata = { :exception => e.inspect, :backtrace => e.backtrace, :raw_messages => message.to_hash }
        @logger.warn "Exception while forwarding message: #{e}", edata
        Hastur.event "hastur.router.exception", e.to_s, 'hastur-admin', MultiJson.dump(edata)
        raise e
      ensure
        # close all of the zmq messages or we might leak C memory
        message.zmq_parts.each do |m| m.close end
      end

      #
      # read from the return socket, insert the socket ID, write to the router socket
      #
      def forward_return_to_router
        begin
          message = read_from @return_socket

          if message
            envelope = Hastur::Envelope.parse message[-2].copy_out_string
            agent_id = @agents[envelope.from]
            message.unshift ::ZMQ::Message.new(agent_id)
            send_to @router_socket, message
            @counters['messages.returned'] += 1
          end
        rescue
          record_message_exception message, e
        ensure
          message.each do |m| m.close end
        end
      end
    end
  end
end
