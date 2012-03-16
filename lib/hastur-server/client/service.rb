require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'uuid'
require 'socket'
require 'termite'

require "hastur"
require "hastur-server/version"
require "hastur-server/util"
require "hastur-server/plugin/v1"
require "hastur-server/input/json"
require "hastur-server/input/statsd"
require "hastur-server/input/collectd"
require "hastur-server/message"

module Hastur
  class Client
    class Service
      attr_reader :uuid, :routers, :port, :heartbeat, :ack_interval

      def initialize(opts)
        raise ArgumentError.new ":uuid is required" unless opts[:uuid] 
        raise ArgumentError.new ":uuid must be in 36-byte hex form" unless Hastur::Util.valid_uuid?(opts[:uuid])
        raise ArgumentError.new ":routers is required" unless opts[:routers]
        raise ArgumentError.new ":routers must be a list" unless opts[:routers].kind_of? Enumerable

        opts[:routers].each do |r|
          raise ArgumentError.new "router '#{r}' is not a valid URI" unless Hastur::Util.valid_zmq_uri?(r)
        end

        opts[:port]           ||= 8125
        opts[:heartbeat]      ||= 30
        opts[:ack_interval]   ||= 30
        opts[:stats_interval] ||= 5

        raise ArgumentError.new ":port must be an integer" unless opts[:port].kind_of? Fixnum
        raise ArgumentError.new ":port must be between 1025 and 65535" unless opts[:port].between? 1025, 65535

        raise ArgumentError.new ":heartbeat must be a number" unless opts[:heartbeat].kind_of? Numeric
        raise ArgumentError.new ":heartbeat must be between 1.0 and 300.0" unless opts[:heartbeat].between? 1, 300

        @acks              = {}
        @plugins           = {}
        @logger            = Termite::Logger.new
        @ctx               = ZMQ::Context.new
        @ack_interval      = opts[:ack_interval]
        @uuid              = opts[:uuid]
        @routers           = opts[:routers]
        @port              = opts[:port]
        @unix              = opts[:unix] # can use a unix socket for testing, should never see production
        @heartbeat         = opts[:heartbeat] * 1_000_000 # microseconds
        @stats_interval    = opts[:stats_interval]
        @last_heartbeat    = Hastur::Util.timestamp - @heartbeat
        @last_ack_check    = Time.now - @ack_interval
        @last_client_reg   = Time.now - 129600 # 1.5 days
        @last_stat_flush   = Time.now

        @counters = {
          :udp_packets => 0,
          :zmq_send    => 0,
          :zmq_recv    => 0,
          :errors      => 0,
          :events      => 0,
        }

        # set the hastur client UDP port to match the listening port
        Hastur.udp_port = @port
      end

      def _fail(message, e)
        @logger.debug "FAIL: #{message}: #{e.inspect}"
        error = Hastur::Message::Error.new :from => @uuid, :payload => e
        error.send(@router_socket)
        @counters[:zmq_send] += 1
        @counters[:errors] += 1
      end

      def _send(message)
        message.send(@router_socket)
        @counters[:zmq_send] += 1
      end

      #
      # use the '_route' field in a JSON hash to choose a Hastur class via
      # Hastur::Message's route_class message (which uses a table of route => class)
      #
      def hash_to_message(data)
        route = data.delete(:_route).to_sym

        if klass = Hastur::Message.symbol_to_class(route)
          payload = MultiJson.encode(data)
        else
          payload = data[:payload]
        end

        klass.new :from => @uuid, :payload => payload
      end

      #
      # Processes a raw network message that was sent to hastur client. The Hastur::Input::*
      # classes all return the same hash structure, suitable for hash_to_message().
      #
      def raw_to_hastur_message(data)
        if jmsg = Hastur::Input::JSON.decode(data)
          _send hash_to_message(jmsg)
        elsif smsg = Hastur::Input::Statsd.decode(data)
          _send hash_to_message(smsg)
        elsif cmsg = Hastur::Input::Collectd.decode(data)
          cmsg.each do |msg|
            _send hash_to_message(msg)
          end
        else
          raise Hastur::UnsupportedError.new "Cannot route raw message: '#{data}'"
        end
      end

      #
      # Read one message from the UDP socket and route it to the message bus. If
      # there are any problems, # a Hastur::Message::Error is created and sent.
      #
      def poll_udp
        # process UDP input from localhost
        begin
          data, sender = @udp_socket.recvfrom_nonblock(65536) rescue nil
          return if data.nil? or data.length == 0
          @counters[:udp_packets] += 1
        rescue Exception => e
          _fail "error reading from UDP socket", e
        end

        @logger.debug "Received UDP message: #{data.inspect}"

        begin
          raw_to_hastur_message(data)
        rescue
          _fail "Received unrecognized UDP message", data
        end
      end

      #
      # After some timeout, all collected stats are sent to hastur
      #
      def send_client_stats
        curr_time = Time.now

        if curr_time - @last_stat_flush > @stats_interval
          t = Process.times
          # amount of user/system cpu time in seconds
          Hastur.gauge("hastur.client.utime", t.utime, curr_time)
          Hastur.gauge("hastur.client.stime", t.stime, curr_time)
          # completed child processes' (plugins) user/system cpu time in seconds (always 0 on Windows NT)
          Hastur.gauge("hastur.client.cutime", t.cutime, curr_time)
          Hastur.gauge("hastur.client.cstime", t.cstime, curr_time)

          @counters.each do |name,count|
            if count > 0
              Hastur.counter("hastur.client.#{name}", count, curr_time)
              @counters[name] = 0
            end
          end

          # reset things
          @last_stat_flush = curr_time
        end
      end

      #
      # Cycles through the plugins that are in question, and sends messages to Hastur
      # if the plugin is done with its execution.
      #
      def poll_plugin_pids
        @plugins.each do |pid,plugin|
          if plugin.done?
            plugin_hash = plugin.to_hash
            Hastur.heartbeat(plugin_hash[:name], nil, nil, nil, plugin_hash)
            @counters[:zmq_send] += 1

            # TODO: call plugin.stat (when it's ready) and send along a stat too

            @plugins.delete pid
          end
        end
      end

      #
      # Sets up the local UDP and TCP sockets. Services communicate with the client through these sockets.
      #
      def set_up_local_ports
        if @unix
          @udp_socket = Socket.new(:UNIX, :DGRAM, 0)
          address = Addrinfo.unix(@unix)
          @udp_socket.bind(address)
        else
          @udp_socket = UDPSocket.new
          @udp_socket.bind nil, @port 
        end
        @logger.debug "Binding UDP socket localhost:#{@port}"
        @tcp_socket = nil
      end

      #
      # Sets up a socket that can communicate with multiple routers.
      #
      def set_up_router
        @router_socket = @ctx.socket(ZMQ::DEALER)
        @routers.each do |router_uri|
          @router_socket.connect(router_uri)
        end
      end

      #
      # Initialize all of the objects needed to perform polling.
      #
      def set_up_poller
        @poller = ZMQ::Poller.new
        if @router_socket
          @poller.register_readable @router_socket
          #@poller.register_writable @router_socket
        end
      end

      #
      # Polls the router socket to read messages that come from Hastur. Also polls the UDP
      # socket to read the messages that come from Services.
      #
      def poll_zmq
        @poller.poll_nonblock

        # read messages from Hastur (router)
        if @poller.readables.include?(@router_socket)
          msg = Hastur::Message.recv(@router_socket)
          @counters[:zmq_recv] += 1
          case msg
          when Hastur::Message::Ack
            ack_key = msg.acked.to_s
            if @acks.has_key? ack_key
              @acks.delete ack_key
            else
              Hastur::Message::Error.new :from => @uuid,
                :payload => "Received an unexpected ack with ID: '#{ack_key}'"
            end
          when Hastur::Message::PluginExec
            # TODO: add hmac authentication of plugin exec messages
            config = msg.decode
            plugin = Hastur::Plugin::V1.new(config[:plugin_path], config[:plugin_args], config[:plugin])
            pid = plugin.run
            @plugins[pid] = plugin
          else
            Hastur::Message::Error.new :from => @uuid,
              :payload => "Received an unsupported #{msg.class} message."
          end
        end
      end

      #
      # Registers a client with Hastur.
      #
      def poll_registration_timeout
        # re-register the client once a day
        if Time.now - @last_client_reg > 86400
          reg_info = {
            :from      => @uuid,
            :source    => self.class.to_s,
            :hostname  => Socket.gethostname,
            :ipv4      => IPSocket.getaddress(Socket.gethostname),
            :type      => "client",
            :timestamp => ::Hastur::Util.timestamp
          }

          msg = Hastur::Message::Registration.new :from => @uuid, :data => reg_info

          @logger.debug "Attempting to register client #{@uuid}: #{msg.to_json}"
          msg.send @router_socket
          @counters[:zmq_send] += 1
          @last_client_reg = Time.now
        end
      end

      #
      # Check to see if it's time to send a heartbeat.
      #
      def poll_heartbeat_timeout
        now = Hastur::Util.timestamp
        delta = now - @last_heartbeat

        # perform heartbeat check
        if delta > @heartbeat
          @logger.debug "Sending heartbeat"

          msg = Hastur::Message::Heartbeat.new(
            :from => @uuid,
            :data => {
              :name           => "hastur.client.heartbeat",
              :value          => delta,
              :timestamp      => now,
              :labels         => {
                :version => Hastur::VERSION,
                :period  => @heartbeat,
              }
            }
          )
          msg.send(@router_socket)

          @last_heartbeat = now
        end
      end

      #
      # Check if any acks are overdue, resend messages that have timed out acks.
      #
      def poll_ack_timeouts
        # perform resends if necessary
        if not @acks.empty? and Time.now - @last_ack_check > @ack_interval
          @logger.debug "Checking unacked messages #{@acks.inspect}"
          @acks.each_pair do |key, msg|
            msg.envelope.incr_resend # record the fact that this is a resend
            msg.send(@router_socket)
          end
          @last_ack_check = Time.now
        end
      end

      #
      # Run the main loop.
      #
      def run
        set_up_local_ports
        set_up_router
        set_up_poller

        @running = true
        while @running
          poll_registration_timeout
          poll_heartbeat_timeout
          poll_ack_timeouts
          poll_plugin_pids
          poll_udp
          poll_zmq
          send_client_stats

          sleep 0.1 # prevent tight loops from using too much CPU
        end

        msg = Hastur::Message::Log.new :from => @uuid, :payload => "Client #{@uuid} exiting."
        msg.send(@router_socket)
        @counters[:zmq_send] += 1

        @router_socket.close
      end

      #
      # Sets a variable so run()'s loop will exit on its next iteration.
      #
      def shutdown
        @logger.debug "Setting running to false."
        @running = false
      end
    end
  end
end
