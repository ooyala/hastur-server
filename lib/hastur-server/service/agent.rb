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
      attr_reader :uuid, :routers, :port, :heartbeat, :ack_interval, :noop_interval, :ohai_info

      #
      # Create a new Hastur Agent. This is the guts of the hastur-agent daemon, sans process setup, etc.
      # @param [Hash{Symbol => String,Fixnum,TrueClass}] opts
      # @option [String] :uuid required, 36-byte agent UUID (usually read from /etc/uuid, see bin/hastur-agent.rb)
      # @option [String] :routers required, list of Hastur routers ZeroMQ URI's
      # @option [String] :unix optional, a unix-domain socket.  If present, don't open regular socket
      # @option [Fixnum] :port default 8125 UDP port to listen on localhost
      # @option [Fixnum] :heartbeat default 30 seconds between heartbeats
      # @option [Fixnum] :ohai_info default 3600 seconds between sending ohai_info
      # @option [Fixnum] :agent_reg default 3600 seconds between sending agent registration
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
        opts[:ohai_info]      ||= 3600
        opts[:agent_reg]      ||= 3600
        opts[:ack_interval]   ||= 30
        opts[:noop_interval]  ||= 30
        opts[:stats_interval] ||= 300

        raise ArgumentError.new ":port must be an integer" unless opts[:port].kind_of? Fixnum
        raise ArgumentError.new ":port must be between 1025 and 65535" unless opts[:port].between? 1025, 65535

        raise ArgumentError.new ":heartbeat must be a number" unless opts[:heartbeat].kind_of? Numeric
        raise ArgumentError.new ":heartbeat must be between 1.0 and 300.0" unless opts[:heartbeat].between? 1, 300

        raise ArgumentError.new ":ohai_info must be a number" unless opts[:ohai_info].kind_of? Numeric
        raise ArgumentError.new ":ohai_info must be between 1.0 and 86400.0" unless opts[:ohai_info].between? 1, 86400

        raise ArgumentError.new ":agent_reg must be a number" unless opts[:agent_reg].kind_of? Numeric
        raise ArgumentError.new ":agent_reg must be between 1.0 and 86400.0" unless opts[:agent_reg].between? 1, 86400

        @acks              = {}
        @logger            = opts[:logger] || Termite::Logger.new
        @ctx               = ZMQ::Context.new
        @poller            = ZMQ::Poller.new
        @ack_interval      = opts[:ack_interval]
        @noop_interval     = opts[:noop_interval]
        @uuid              = opts[:uuid]
        @routers           = opts[:routers]
        @port              = opts[:port]
        @unix              = opts[:unix] # can use a unix socket for testing, should never see production
        @heartbeat         = opts[:heartbeat]
        @ohai_info         = opts[:ohai_info]
        @agent_reg         = opts[:agent_reg]
        @stats_interval    = opts[:stats_interval]
        @no_heartbeat      = opts[:no_heartbeat]
        @no_ohai_info      = opts[:no_ohai_info]
        @no_agent_reg      = opts[:no_agent_reg]
        @no_agent_stats    = opts[:no_agent_stats]
        @no_system_stats   = opts[:no_system_stats]

        @startup           = Time.now
        @last_heartbeat    = @startup - opts[:heartbeat]
        @last_ack_check    = @startup - @ack_interval
        @last_noop_blast   = @startup - @noop_interval
        @last_agent_reg    = @startup - opts[:agent_reg] # no delay
        @last_ohai_info    = @startup - opts[:ohai_info] + 60  # 60 second delay
        @last_stat_flush   = @startup
        @last_system_flush = @startup

        @counters = {
          :udp_packets => 0,
          :zmq_send    => 0,
          :zmq_recv    => 0,
          :errors      => 0,
          :noops       => 0,
          :events      => 0,
        }

        override_hastur_sender

        @logger.info "Hastur agent up and running."
      end

      # hand a block to Hastur client so it can send directly over ZMQ instead of
      # sendto UDP -> OS -> itself -> ZMQ
      def override_hastur_sender
        Hastur.deliver_with do |m|
          begin
            _send(hash_to_message(m))
          rescue Exception => e
            _fail(m, e)
          end
        end
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
      def poll_udp(now)
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
      def send_agent_stats(now, immediate=false)
        if (now - @last_stat_flush > @stats_interval) or immediate
          t = Process.times
          # amount of user/system cpu time in seconds
          Hastur.gauge("hastur.agent.utime", t.utime, now)
          Hastur.gauge("hastur.agent.stime", t.stime, now)
          # completed child processes' user/system cpu time in seconds (always 0 on Windows NT)
          Hastur.gauge("hastur.agent.cutime", t.cutime, now)
          Hastur.gauge("hastur.agent.cstime", t.cstime, now)

          @counters.each do |name,count|
            if count > 0
              Hastur.counter("hastur.agent.#{name}", count, now)
              @counters[name] = 0
            end
          end

          # reset things
          @last_stat_flush = now
        end
      end

      def send_system_stats(now)
        # send system stats every 10 seconds
        if (now - @last_system_flush) >= 10
          # currently, only Linux is supported, others could be added here
          if File.exists?("/proc/net/dev")
            Hastur::Agent::LinuxStats.run
          end
          @last_system_flush = now
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
      # Polls the router socket to read messages that come from Hastur.
      #
      def poll_zmq(now)
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
          else
            Hastur::Message::Error.new :from => @uuid,
              :payload => "Received an unsupported #{msg.class} message."
          end
          # poll again or be stuck in this loop waiting for the next ZMQ message
          @poller.poll_nonblock
        end
      end

      #
      # Registers an agent with Hastur.
      #
      def poll_registration_timeout(now)
        delta = now - @last_agent_reg

        if delta > @agent_reg
          @logger.debug "Sending agent registration"

          reg_info = {
            :from      => @uuid,
            :source    => self.class.to_s,
            :hostname  => Socket.gethostname,
            :ipv4      => (IPSocket.getaddress(Socket.gethostname) rescue "127.0.0.1"),
            :timestamp => ::Hastur::Util.timestamp,
            :labels => Hastur.send(:default_labels).merge(:period => @agent_reg)
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

          @last_agent_reg = now
        end
      end

      #
      # Sends Ohai info to Hastur.
      #
      def poll_ohai_info_timeout(now)
        delta = now - @last_ohai_info

        if delta > @ohai_info
          begin
            @logger.debug "Sending OHAI info"

            ohai = Ohai::System.new
            ohai.all_plugins
            # Hastur requires all bodies to have timestamps, and we like them to have UUID's and labels
            info = ohai.data.merge({
              "uuid" => @uuid,
              "timestamp" => Hastur.timestamp,
              "labels" => Hastur.send(:default_labels).merge(:period => @ohai_info),
            })
            msg = Hastur::Message::Info::Ohai.new :from => @uuid, :data => info
            _send(msg)
          rescue Exception => e
            @logger.info "ohai failed: #{e}"
          end
          @last_ohai_info = now
        end
      end

      #
      # Check to see if it's time to send a heartbeat.
      #
      def poll_heartbeat_timeout(now)
        delta = now - @last_heartbeat

        # perform heartbeat check
        if delta > @heartbeat
          @logger.debug "Sending heartbeat"

          msg = Hastur::Message::HB::Agent.new(
            :from => @uuid,
            :data => {
              :name           => "hastur.agent.heartbeat",
              :value          => delta,
              :timestamp      => Hastur.timestamp,
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
      def poll_ack_timeouts(now)
        # perform resends if necessary
        if not @acks.empty? and now - @last_ack_check > @ack_interval
          @logger.debug "Checking unacked messages #{@acks.inspect}"
          @acks.each_pair do |key, msg|
            msg.envelope.incr_resend # record the fact that this is a resend
            _send(msg)
          end
          @last_ack_check = now
        end
      end

      #
      # send out (number of routers) messages periodically to make sure they have routes cached
      #
      def poll_noop(now)
        if now - @last_noop_blast > @noop_interval
          @routers.count.times do
            _send(Hastur::Message::Noop.new(:from => @uuid))
            @counters[:noops] += 1
          end
          @last_noop_blast = now
        end
      end

      #
      # To clean up, e.g. for a restart, shutdown() must be called to tear down UDP & ZeroMQ sockets.
      #
      def setup
        # wait 1s after close/shutdown to send up to 10,000 pending messages
        # wait 1s between attempts to reconnect to a hastur router - there are always >= 2 of them
        sockopts = { :linger => 1_000, :hwm => 10_000, :reconnect_ivl => 1_000 }
        @router_socket = Hastur::Util.connect_socket @ctx, ZMQ::DEALER, @routers, sockopts
        @poller.register_readable @router_socket

        set_up_local_ports

        @running = true
      end

      #
      # Run the main loop. setup() must be called first.
      #
      def run
        last_heartbeat_time = Time.now - 61

        @logger.fatal "run() called before setup()" unless @running
        while @running
          now = Time.now

          poll_noop now
          poll_registration_timeout now unless @no_agent_reg
          poll_ohai_info_timeout now    unless @no_ohai_info
          poll_heartbeat_timeout now    unless @no_heartbeat
          poll_ack_timeouts now
          poll_zmq now
          send_agent_stats(now)         unless @no_agent_stats
          send_system_stats(now)        unless @no_system_stats

          if @running and select([@udp_socket], [], [], 0.25)
            poll_udp now
          end

          # agent doesn't use the Hastur background thead, send a heartbeat every minute
          if (now - last_heartbeat_time) >= 60
            Hastur.heartbeat("hastur.agent.process_heartbeat")
            last_heartbeat_time = now
          end
        end

        Hastur.log "Hastur Agent #{@uuid} exiting.", {
          :uptime => (Time.now - @startup),
          :startup => @startup,
          :shutdown => Time.now
        }

      # try to report to the mothership when an exception occurs
      rescue => exception
        @running = false
        @counters[:errors] += 1
        error = Hastur::Message::Error.new :from => @uuid, :data => exception
        _send(error)
        send_agent_stats now, true
        @logger.warn "Exiting agent due to an exception: #{exception.inspect}\n#{exception.backtrace.join("\n")}"
        sleep 1.0 # give zeromq some time to hand off messages
      end

      #
      # Set the run flag to false and let the run loop exit gracefully.
      #
      def stop
        @logger.info "Hastur agent shutting down normally."
        @running = false
      end

      #
      # Set the run flag to false and shut down all the sockets. This can cause
      # (mostly) harmless exceptions if it's called in the middle of the run loop
      # and therefore should not be called for normal exit.
      # @see :stop
      #
      def shutdown
        if @running == false
          @logger.info "Hastur agent sockets shutting down gracefully."
        else
          @logger.warn "Hastur agent sockets shutting down **forcefully**."
          # make sure the run loop exits, there will be exceptions
          @running = false
        end

        @router_socket.close
        @udp_socket.close
        @ctx.terminate
      end
    end
  end
end
