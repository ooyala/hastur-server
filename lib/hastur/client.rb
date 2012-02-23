require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'uuid'
require 'socket'
require 'termite'

require "hastur"
require "hastur/util"
require "hastur/plugin/v1"
require "hastur/input/json"
require "hastur/input/statsd"
require "hastur/input/collectd"
require "hastur/message"

module Hastur
  class Client
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
      @heartbeat         = opts[:heartbeat]
      @stats_interval    = opts[:stats_interval]
      @num_msgs          = 0
      @num_notifications = 0
      @last_heartbeat    = Time.now - @heartbeat
      @last_ack_check    = Time.now - @ack_interval
      @last_client_reg   = Time.now - 129600 # 1.5 days
      @last_stat_flush   = Time.now
    end

    #
    # use the '_route' field in a JSON hash to choose a Hastur route
    # and send the JSON along as the payload in a routed Hastur message
    #
    def route_json(data)
      route = data.delete :_route
      if klass = Hastur::Message.route_class(route)
        # TODO: enable messages being able to request acks without breaking notification acks
        # TODO: this is a bit primitive, needs to be smarter for various message types
        if klass.json?
          payload = MultiJson.encode(data)
        else
          payload = data[:payload]
        end

        begin
          msg = klass.new :from => @uuid, :payload => payload
          msg.send @router_socket

          # invalid messages that cause exceptions and have acks won't make it this far
          # TODO: what to do here?
          if ack or msg.envelope.ack?
            @acks[msg.envelope.to_s] = msg
          end
        rescue
          # TODO: send an error, log, etc.
        end
      else
        e = Hastur::UnsupportedError.new "Cannot route JSON: #{json}"
        Hastur::Message::Error.new(:from => @uuid, :payload => e.to_s).send(@router_socket)
        throw e
      end
    end

    #
    # Processes a random UDP message that was sent to the client. For now,
    # the message simply gets forwarded on to the message bus.
    #
    def poll_udp
      # process UDP input from localhost
      data, sender = @udp_socket.recvfrom_nonblock(65536) rescue nil
      return if data.nil? or data.length == 0

      @logger.debug "Received UDP message: #{data.inspect}"

      # records stats about the message
      @num_msgs += 1

      if msg = Hastur::Input::JSON.decode(data)
        @num_notifications += 1 if msg[:_route] == "notification"
        route_json(msg)
      elsif msg = Hastur::Input::Statsd.decode(data)
        route_json(msg)
      elsif msg = Hastur::Input::Collectd.decode(data)
        msg.each { |m| route_json(m) }
      else
        @logger.debug "Received unrecognized (not JSON or statsd) packet: #{msg.inspect}"
        error = Hastur::Message::Error.new :from => @uuid, :payload => "invalid data on UDP port #{@port}: '#{data}'"
        error.send(@router_socket)
      end
    end

    #
    # After some timeout, all collected stats are sent to hastur
    #
    def poll_stats
      curr_time = Time.now
      # After some timeout, send the incremental difference to Hastur
      if curr_time - @last_stat_flush > @stats_interval
        t = Process.times
        # amount of user cpu time in seconds
        Hastur.gauge("client.utime", t.utime, curr_time)
        # amount of system cpu time in seconds
        Hastur.gauge("client.stime", t.stime, curr_time)
        # completed child processes' user cpu time in seconds (always 0 on Windows NT)
        Hastur.gauge("client.cutime", t.cutime, curr_time)
        # completed child processes' system cpu time in seconds (always 0 on Windows NT)
        Hastur.gauge("client.cstime", t.cstime, curr_time)
        Hastur.counter("client.num_msgs", @num_msgs, curr_time)
        Hastur.counter("client.num_notifications", @num_notifications, curr_time)
        # reset things
        @last_stat_flush = curr_time
        @num_msgs = 0
        @num_notifications = 0
      end
    end

    #
    # Cycles through the plugins that are in question, and sends messages to Hastur
    # if the plugin is done with its execution.
    #
    def poll_plugin_pids
      @plugins.each do |pid,plugin|
        if plugin.done?
          msg = Hastur::Message::PluginResult.new(:from => @uuid, :data => plugin.to_hash)
          msg.send(@router_socket)

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
        @poller.register_writable @router_socket
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
          config = msg.decode
          plugin = Hastur::Plugin::V1.new(config[:plugin_path], config[:plugin_args])
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
        msg = Hastur::Message::RegisterClient.new :from => @uuid
        @logger.debug "Attempting to register client #{@uuid}: #{msg.to_json}"
        msg.send @router_socket
        @last_client_reg = Time.now
      end
    end

    #
    # Check to see if it's time to send a heartbeat.
    #
    def poll_heartbeat_timeout
      # perform heartbeat check
      if Time.now - @last_heartbeat > @heartbeat
        @logger.debug "Sending heartbeat"

        msg = Hastur::Message::HeartbeatClient.new(
          :from => @uuid,
          :data => {
            :last_heartbeat => Hastur::Util.timestamp(@last_heartbeat),
            :heartbeat      => @heartbeat
          }
        )
        msg.send(@router_socket)

        @last_heartbeat = Time.now
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
        poll_stats
        poll_udp
        poll_zmq

        sleep 0.1 # prevent tight loops from using too much CPU
      end

      msg = Hastur::Message::Log.new :from => @uuid, :payload => "Client #{@uuid} exiting."
      msg.send(@router_socket)

      @router_socket.close
    end

    #
    # Sets a variable so run()'s loop will exit on its next iteration.
    #
    def shutdown
      puts "Setting running to false."
      @running = false
    end
  end
end
