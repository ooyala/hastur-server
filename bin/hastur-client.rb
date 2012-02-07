#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'
require 'socket'
require 'termite'

require "hastur/zmq_utils"
require "hastur/client/uuid"
require "hastur/plugin/v1"

MultiJson.engine = :yajl
NOTIFICATION_INTERVAL = 5   # Hardcode for now

opts = Trollop::options do
  opt :router,    "Router URI",         :type => String,  :required => true, :multi => true
  opt :uuid,      "System UUID",        :type => String
  opt :port,      "Local socket port",  :type => String,  :required => true
  opt :heartbeat, "Heartbeat interval", :type => Integer, :default => 15
end

if opts[:router].any? { |uri| uri !~ /\w+:\/\/[^:]+:\d+/ }
  Trollop::die :router, "--router is required and must be in this format: protocol://hostname:port"
end

unless opts[:uuid]
  # attempt to retrieve UUID from disk; UUID gets created on the fly if it doesn't exist
  opts[:uuid] = Hastur::Client::UUID.get_uuid
  puts opts[:uuid]
end

CLIENT_UUID = opts[:uuid]
ROUTERS = opts[:router]
LOCAL_PORT = opts[:port]
HEARTBEAT_INTERVAL = opts[:heartbeat]

@logger = Termite::Logger.new

#
# Processes a random UDP message that was sent to the client. For now,
# the message simply gets forwarded on to the message bus.
#
def process_udp_message(msg)
  @logger.debug "Received UDP message: #{msg.inspect}"

  # check that the message looks like json before calling the decoder
  if msg =~ /\A\s*\{.+\}\s*\z/s
    begin
      hash = MultiJson.decode(msg)
    rescue
      @logger.debug "Received invalid JSON packet: #{msg.inspect}"
      return
    end
  # try statsd protocol
  elsif statsd = Hastur::ZMQ::Utils::STATSD_RE.match(msg)
    # TODO: this is a guess at what our stat format is going to be, make sure to update it if that changes
    hash = {
      "method" => "stat",
      "params" => {
        :name      => "statsd.#{statsd[:name]}",
        :value     => statsd[:value],
        :units     => statsd[:unit],
        :timestamp => Time.now.to_f,
        :tags      => { :source => "statsd", :name => statsd[:name] }
      }
    }
  # TODO: this should forward to the error topic
  else
    @logger.debug "Received unrecognized (not JSON or statsd) packet: #{msg.inspect}"
    return
  end

  # save un-ack'd notifications
  if hash['method'] == "notify"
    if !hash['params'].nil? && hash['params']['id']
      @notifications[hash['params']['id']] = hash
    else
      Hastur::ZMQUtils.hastur_send(
        @router_socket,
        "log", 
        hash.merge('uuid' => CLIENT_UUID, 'message' => "Unable to parse for notification id"))
    end
  end

  # forward the message to the message bus
  Hastur::ZMQUtils.hastur_send(
    @router_socket, hash['method'] || "error", hash.merge('uuid' => CLIENT_UUID))
end

#
# Processes the raw 'schedule' message to retrieve two key components of a plugin.
#   1. plugin_command
#   2. plugin_args
#
def process_schedule_message(message)
  @logger.debug "Cheerfully ignoring multipart to-client message: #{message.inspect}"
  hash = MultiJson.decode(message) rescue nil
  unless hash
    @logger.debug "Received invalid JSON packet: #{msg.inspect}"
    return
  end

  [ hash["plugin_path"], hash["plugin_args"] ]
end

#
# Processes the raw 'notification_ack' message. Removes the notification from the un-ack'd list.
#
def process_notification_ack(msg)
  hash = MultiJson.decode(msg) rescue nil
  unless hash
    @logger.debug "Received invalid JSON packet: #{msg.inspect}"
    return 
  end

  @logger.debug "ACK received for notification [#{hash['id']}]"
  notification = @notifications.delete(hash['id'])
  unless notification
    Hastur::ZMQUtils.hastur_send(@router_socket, "log", 
      { :message => 
      "Unable to ack notification with id #{hash['id']} because it does not exist."})
  end
end

#
# Cycles through the plugins that are in question, and sends messages to Hastur
# if the plugin is done with its execution.
#
def poll_plugin_pids(plugins)
  plugins.each do |pid,plugin|
    if plugin.done?
      # when the process is done, it's safe to slurp the filehandles without blocking
      stdout, stderr = plugin.slurp

      Hastur::ZMQUtils.hastur_send(@router_socket, "stats", {
        :pid    => plugin.pid,
        :status => plugin.status,
        :stdout => stdout,
        :stderr => stderr
      })

      plugins.delete pid
    end
  end
end

#
# Sets up the local UDP and TCP sockets. Services communicate with the client through these sockets.
#
def set_up_local_ports
  @udp_socket = UDPSocket.new
  @logger.debug "Binding UDP socket localhost:#{LOCAL_PORT}"
  @udp_socket.bind nil, LOCAL_PORT
  @tcp_socket = nil
end

#
# Sets up a socket that can communicate with multiple routers.
#
def set_up_router
  @context = ZMQ::Context.new
  @router_socket = @context.socket(ZMQ::DEALER)
  @router_socket.setsockopt(ZMQ::IDENTITY, CLIENT_UUID)
  ROUTERS.each do |router_uri|
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
  @last_heartbeat = Time.now
  @last_notification_check = Time.now
end

#
# Polls the router socket to read messages that come from Hastur. Also polls the UDP
# socket to read the messages that come from Services.
#
def poll_zmq(plugins)
  @poller.poll_nonblock
  # read messages from Hastur
  if @poller.readables.include?(@router_socket)
    msgs = []
    err = @router_socket.recv_strings msgs
    if err < 0
      @logger.debug "Error #{err} reading router socket!"
      return
    end
    case msgs[-2]
    when "schedule"
      plugin_command, plugin_args = process_schedule_message(msgs[-1])
      plugin = Hastur::Plugin::V1.new(plugin_command, plugin_args)
      pid = plugin.run
      plugins[pid] = plugin
    when "notification_ack"
      process_notification_ack msgs[-1] 
    else
      # log error
      Hastur::ZMQUtils.hastur_send(@router_socket, "error",
        {:message => "Unable to deal with this type of message => #{msgs[-2]}"})
    end
  end
  # read messages from Services
  msg, sender = @udp_socket.recvfrom_nonblock(100000) rescue nil  # More than max UDP packet size
  process_udp_message(msg) unless msg.nil? || msg == ""
  # If this throttles too much, adjust downward as needed
  sleep 0.1
  # perform heartbeat check
  if Time.now - @last_heartbeat > HEARTBEAT_INTERVAL
    @logger.debug "Sending heartbeat"
    Hastur::ZMQUtils.hastur_send(@router_socket, "heartbeat",
      { :name => "hastur thin client", :uuid => CLIENT_UUID } )
    @last_heartbeat = Time.now
  end
  # perform notification resends if necessary
  if Time.now - @last_notification_check > NOTIFICATION_INTERVAL && !@notifications.empty?
    @logger.debug "Checking unsent notifications #{@notifications.inspect}"
    @notifications.each_pair do |notification_id, notification|
      Hastur::ZMQUtils.hastur_send(@router_socket, "notify", notification)
    end
    @last_notification_check = Time.now
  end
end

#
# Registers a client with Hastur.
#
def register_client(uuid)
  # register the client
  Hastur::ZMQUtils.hastur_send @router_socket, "register", 
    {
      :params =>
        { :name => CLIENT_UUID,
          :hostname => Socket.gethostname, 
          :ipv4 => IPSocket::getaddress(Socket.gethostname)
        },
      :id => CLIENT_UUID,
      :method => "register_client"
    }
  # log to hastur that we at least attempted to register this client
  Hastur::ZMQUtils.hastur_send(@router_socket, "logs", 
    { :message => "Attempting to register client #{CLIENT_UUID}", :uuid => CLIENT_UUID })
end

#
# Entry point of the thin client.
#
def main
  plugins = {}
  @notifications = {}
  set_up_local_ports
  set_up_router
  set_up_poller
  register_client CLIENT_UUID

  loop do
    poll_plugin_pids(plugins)
    poll_zmq(plugins)
  end
end

main()
