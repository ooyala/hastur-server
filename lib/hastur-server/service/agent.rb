require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'uuid'
require 'socket'
require 'termite'
require 'ohai/system'
require 'sys/uname'

require "hastur/api"
require "hastur-server/version"
require "hastur-server/util"
require "hastur-server/agent/plugin_v1_exec"
require "hastur-server/agent/linux_stats"
require "hastur-server/input/json"
require "hastur-server/input/statsd"
require "hastur-server/input/collectd"
require "hastur-server/message"

Hastur.app_name = "hastur-agent.rb"
Hastur.no_background_thread!

module Hastur
  module Service
    class Agent
      attr_reader :uuid, :routers, :port, :heartbeat, :ack_interval, :noop_interval

      #
      # Create a new Hastur Agent. This is the guts of the hastur-agent daemon, sans process setup, etc.
      # @param [Hash{Symbol => String,Fixnum,TrueClass}] opts
      # @option [String] :uuid required, 36-byte agent UUID (usually read from /etc/uuid, see bin/hastur-agent.rb)
      # @option [String] :routers required, list of Hastur routers ZeroMQ URI's
      # @option [String] :unix optional, a unix-domain socket.  If present, don't open regular socket
      # @option [Fixnum] :port default 8125 UDP port to listen on localhost
      # @option [Fixnum] :heartbeat default 30 seconds between heartbeats
      # @option [Fixnum] :ack_interval default 30 seconds before resending unacked messages
      # @option [Fixnum] :noop_interval default 30 seconds between noop broadcasts
      # @option [Fixnum] :stats_interval default 300 send agent stats every n seconds
      # @option [TrueClass] :no_agent_stats disable sending agent stats when true
      #
      def initialize(opts)
        raise ArgumentError.new ":uuid is required" unless opts[:uuid]
        raise ArgumentError.new ":uuid must be in 36-byte hex form" unless Hastur::Util.valid_uuid?(opts[:uuid])
        raise ArgumentError.new ":routers is required" unless opts[:routers]
        raise ArgumentError.new ":routers must be a list" unless opts[:routers].kind_of? Enumerable

        opts[:routers].each do |r|
          raise ArgumentError.new "router '#{r}' is not a valid URI" unless Hastur::Util.valid_zmq_uri?(r)
        end

        opts[:port]           ||= 8125
        opts[:heartbeat]      ||= 60
        opts[:ack_interval]   ||= 30
        opts[:noop_interval]  ||= 30
        opts[:stats_interval] ||= 300

        raise ArgumentError.new ":port must be an integer" unless opts[:port].kind_of? Fixnum
        raise ArgumentError.new ":port must be between 1025 and 65535" unless opts[:port].between? 1025, 65535

        raise ArgumentError.new ":heartbeat must be a number" unless opts[:heartbeat].kind_of? Numeric
        raise ArgumentError.new ":heartbeat must be between 1.0 and 300.0" unless opts[:heartbeat].between? 1, 300

        @acks              = {}
        @plugins           = {}
        @logger            = opts[:logger] || Termite::Logger.new
        @ctx               = ZMQ::Context.new
        @poller            = ZMQ::Poller.new
        @ack_interval      = opts[:ack_interval]
        @noop_interval     = opts[:noop_interval]
        @uuid              = opts[:uuid]
        @routers           = opts[:routers]
        @port              = opts[:port]
        @unix              = opts[:unix] # can use a unix socket for testing, should never see production
        @heartbeat         = opts[:heartbeat] * 1_000_000 # microseconds
        @stats_interval    = opts[:stats_interval]
        @no_agent_stats    = opts[:no_agent_stats]
        @last_heartbeat    = Hastur::Util.timestamp - @heartbeat
        @last_ack_check    = Time.now - @ack_interval
        @last_noop_blast   = Time.now - @noop_interval
        @last_agent_reg    = Time.now - 86400 # no delay
        @last_ohai_info    = Time.now - 86340 # 60 second delay
        @last_stat_flush   = Time.now

        @counters = {
          :udp_packets => 0,
          :zmq_send    => 0,
          :zmq_recv    => 0,
          :errors      => 0,
          :noops       => 0,
          :events      => 0,
        }

        # hand a block to Hastur client so it can send directly over ZMQ instead of
        # sendto UDP -> OS -> itself -> ZMQ
        Hastur.deliver_with do |m|
          begin
            _send(hash_to_message(m))
          rescue Exception => e
            _fail(m, e)
          end
        end

        @logger.info "Hastur agent up and running."
      end

      def _fail(message, e)
        @logger.debug "FAIL: #{message}: #{e.inspect}"
        error = Hastur::Message::Error.new :from => @uuid, :data => e
        _send(error)
        @counters[:errors] += 1
      end

      def _send(message)
        message.send(@router_socket)
        @counters[:zmq_send] += 1
      end

      #
      # use the type field in a JSON hash to choose a Hastur class
      # @param [Hash] data a hash of options for building a hastur message
      #
      def hash_to_message(data)
        klass = Hastur::Message.symbol_to_class(data[:type])

        # if no UUID is provided, set one in the message payload
        data[:uuid] ||= @uuid

        payload = MultiJson.dump(data)
        klass.new :from => @uuid, :payload => payload
      end

      #
      # Processes a raw network message that was sent to hastur agent. The Hastur::Input::*
      # classes all return the same hash structure, suitable for hash_to_message().
      #
      def raw_to_hastur_message(data)
        if jmsg = Hastur::Input::JSON.decode(data)
          _send hash_to_message(jmsg)
        elsif smsg = Hastur::Input::Statsd.decode(data)
          _send hash_to_message(smsg)
        elsif cmsg = Hastur::Input::Collectd.decode(data)
          # collectd packets usually contain multiple values, break them up
          cmsg.each do |msg|
            _send hash_to_message(msg)
          end
        else
          raise Hastur::UnsupportedError.new "Cannot determine type of raw message: '#{data}'"
        end
      end

      #
      # Read one message from the UDP socket and route it to the message bus. If
      # there are any problems, # a Hastur::Message::Error is created and sent.
      #
      def poll_udp
        # process UDP input from localhost
        begin
          data, sender = @udp_socket.recvfrom_nonblock(65535) rescue nil
          return if data.nil? or data.length == 0
          @counters[:udp_packets] += 1
        rescue Exception => e
          _fail "error reading from UDP socket", e
        end

        begin
          raw_to_hastur_message(data)
        rescue
          _fail "Received unrecognized UDP message", data
        end
      end

      #
      # After some timeout, all collected stats are sent to hastur
      #
      def send_agent_stats
        curr_time = Time.now

        if curr_time - @last_stat_flush > @stats_interval
          t = Process.times
          # amount of user/system cpu time in seconds
          Hastur.gauge("hastur.agent.utime", t.utime, curr_time)
          Hastur.gauge("hastur.agent.stime", t.stime, curr_time)
          # completed child processes' (plugins) user/system cpu time in seconds (always 0 on Windows NT)
          Hastur.gauge("hastur.agent.cutime", t.cutime, curr_time)
          Hastur.gauge("hastur.agent.cstime", t.cstime, curr_time)

          @counters.each do |name,count|
            if count > 0
              Hastur.counter("hastur.agent.#{name}", count, curr_time)
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
            msg = Hastur::Message::HB::PluginV1.new(
              :from => @uuid,
              :data => {
                :name           => plugin_hash[:name],
                :value          => nil, # should this be the resultcode?
                :timestamp      => nil, # take the default
                :labels         => plugin_hash
              }
            )
            _send msg
            # TODO: call plugin.stat (when it's ready) and send along a stat too
            @plugins.delete pid
          end
        end
      end

      #
      # Sets up the local UDP socket. Services communicate with the agent through these sockets.
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
      # Polls the router socket to read messages that come from Hastur. Also polls the UDP
      # socket to read the messages that come from Services.
      #
      def poll_zmq
        @poller.poll_nonblock

        # read messages from Hastur (router)
        while @poller.readables.include?(@router_socket)
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
          when Hastur::Message::Cmd::PluginV1
            # TODO: add hmac authentication of plugin exec messages
            config = msg.decode
            plugin = Hastur::Agent::PluginV1Exec.new(config[:plugin_path], config[:plugin_args], config[:plugin])
            pid = plugin.run
            @plugins[pid] = plugin
          else
            Hastur::Message::Error.new :from => @uuid,
              :payload => "Received an unsupported #{msg.class} message."
          end
        end
      end

      #
      # Registers an agent with Hastur.
      #
      def poll_registration_timeout
        # re-register the agent once a day
        if Time.now - @last_agent_reg > 86400
          reg_info = {
            :from      => @uuid,
            :source    => self.class.to_s,
            :hostname  => Socket.gethostname,
            :ipv4      => IPSocket.getaddress(Socket.gethostname),
            :timestamp => ::Hastur::Util.timestamp
          }

          begin
            uname = Sys::Uname.uname
            # nodename is the kernel's idea of its network name, which isn't always the same as hostname
            reg_info[:nodename] = uname.nodename
            reg_info[:sysname]  = uname.sysname
            reg_info[:machine]  = uname.machine
          rescue Exception => e
            @logger.info "Could not call uname(2): #{e}", e.backtrace
          end

          # this is an Ooyala standard file for setting the user-facing hostname
          # it should be stored and always returned first in the cname list if it's set
          if File.exists?("/etc/cnames")
            cnames = File.read("/etc/cnames") rescue ""
            reg_info[:etc_cnames] = cnames.split(/\s+/)
          end

          @logger.debug "Attempting to register agent #{@uuid}", reg_info

          msg = Hastur::Message::Reg::Agent.new :from => @uuid, :data => reg_info
          _send(msg)

          @last_agent_reg = Time.now
        end
      end

      #
      # Sends Ohai info to Hastur.
      #
      def poll_ohai_info_timeout
        if Time.now - @last_ohai_info > 86400
          begin
            ohai = Ohai::System.new
            ohai.all_plugins
            # Hastur requires all bodies to have timestamps, and we like them to have UUID's and labels
            info = ohai.data.merge({
              "uuid" => @uuid,
              "timestamp" => Hastur.timestamp,
              "labels" => Hastur.send(:default_labels),
            })
            msg = Hastur::Message::Info::Ohai.new :from => @uuid, :data => info
            _send(msg)
          rescue Exception => e
            @logger.info "ohai failed: #{e}"
          end
          @last_ohai_info = Time.now
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

          msg = Hastur::Message::HB::Agent.new(
            :from => @uuid,
            :data => {
              :name           => "hastur.agent.heartbeat",
              :value          => delta,
              :timestamp      => now,
              :labels         => {
                :version => Hastur::SERVER_VERSION,
                :period  => @heartbeat,
              }
            }
          )
          _send(msg)

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
            _send(msg)
          end
          @last_ack_check = Time.now
        end
      end

      #
      # send out (number of routers) messages periodically to make sure they have routes cached
      #
      def poll_noop
        if Time.now - @last_noop_blast > @noop_interval
          @routers.count.times do
            _send(Hastur::Message::Noop.new(:from => @uuid))
            @counters[:noops] += 1
          end
          @last_noop_blast = Time.now
        end
      end

      #
      # Run the main loop.
      #
      def run
        @router_socket = Hastur::Util.connect_socket @ctx, ZMQ::DEALER, @routers, :linger => 1, :hwm => 10_000
        @poller.register_readable @router_socket

        set_up_local_ports

        Hastur.start
        @running = true

        last_system_stat_time = Time.now
        last_heartbeat_time = Time.now - 61

        while @running
          poll_noop
          poll_registration_timeout
          poll_ohai_info_timeout
          poll_heartbeat_timeout
          poll_ack_timeouts
          poll_plugin_pids
          poll_zmq rescue nil # Temp: 2012-05-02, should properly detect & log bad messages
          send_agent_stats unless @no_agent_stats

          if select([@udp_socket], [], [], 0.25)
            poll_udp
          end

          now = Time.now
          # agent doesn't use the Hastur background thead, send a heartbeat every minute
          if (now - last_heartbeat_time) >= 60
            Hastur.heartbeat("hastur.agent.process_heartbeat")
            last_heartbeat_time = now
          end

          # send Linux stats every 10 seconds
          if (now - last_system_stat_time) >= 10 and File.exists?("/proc/net/dev")
            Hastur::Agent::LinuxStats.run
            last_system_stat_time = now
          end
        end

        msg = Hastur::Message::Log.new :from => @uuid, :payload => "Hastur Agent #{@uuid} exiting."
        _send(msg)

        @router_socket.close
      end

      #
      # Sets a variable so run()'s loop will exit on its next iteration.
      #
      def shutdown
        @logger.info "Hastur agent shutting down normally."
        @running = false
        @router_socket.close
        @udp_socket.close
        @ctx.terminate
      end
    end
  end
end
