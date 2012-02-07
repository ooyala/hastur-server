#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'socket'
require 'trollop'
require 'termite'

$LOAD_PATH << File.join(__FILE__, "..", "..", "lib")
require "hastur/zmq_utils"
require "hastur/message"

Ecology.read("hastur-router.ecology")

logger = Termite::Logger.new

MultiJson.engine = :yajl

METHODS = [ :register, :notify, :stats, :heartbeat, :logs ]

opts = Trollop::options do
  banner <<-EOS
basic-router.rb - a simple 0mq router.  Clients connect to the router URI,
  sinks connect to the sink URIs.

  Options:
EOS
  opt :router_uri,          "ZMQ Router (incoming) URI", :default => "tcp://127.0.0.1:4321", :type => String
  opt :from_client_pub_uri, "ZMQ Pub URI for sinks",     :default => "tcp://127.0.0.1:4322", :type => String
  opt :to_client_pub_uri,   "ZMQ Pub URI for sinks",     :default => "tcp://127.0.0.1:4320", :type => String
  opt :from_sink_uri,       "ZMQ from-sink URI",         :default => "tcp://127.0.0.1:4323", :type => String

  port = 4330
  METHODS.each do |method|
    opt "#{method}_uri".to_sym, "ZMQ #{method} sink URI",
      :default => "tcp://127.0.0.1:#{port}", :type => String
    port += 1
  end
  
  opt :error_uri, "ZMQ Error sink URI", :default => "tcp://127.0.0.1:4350", :type => String
  opt :linger,    "set ZMQ_LINGER",     :default => 1,                      :type => Integer
  opt :hwm,       "set ZMQ_HWM",        :default => 1,                      :type => Integer
  opt :timeout,   "poll timeout",       :default => 0.1
end

method_uris = METHODS.map(&:to_s).map { |s| s + "_uri" }.map(&:to_sym)

(method_uris + [:router_uri, :from_client_pub_uri, :to_client_pub_uri, :error_uri]).each do |opt|
  if opts[opt] !~ /\w+:\/\/[^:]+:\d+/
    Trollop::die opt, "Option --#{opt} must be of the form protocol://hostname:port rather than #{opts[opt]}"
  end

  if ZMQ::LibZMQ.version2? && opts[opt] =~ /\Wlocalhost\W/
    Trollop::die opt, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
  end
end

# ZeroMQ setup
version_hash = ZMQ::LibZMQ.version
version = "#{version_hash[:major]}.#{version_hash[:minor]}p#{version_hash[:patch]}"
logger.info "Hastur Router (#{Process.pid}) -- Using ZeroMQ version #{version}"

ctx = ZMQ::Context.new(1)

def process_messages_for_routing(messages)
  destination = "error"
  if messages.size > 1
    routing_envelope = messages[-2]
    if routing_envelope[0] == "v"
      # Versioned routing envelope.  Perfect!
      version, destination, ackability = routing_envelope.split("\n")
    else
      destination = routing_envelope
    end
  end
  hostname = Socket.gethostname
  # TODO(noah): Add more to envelope
  router_envelope = "#{hostname}"
  messages.unshift router_envelope
  destination
end

sockets = {}
logger.debug "Setting up router socket on #{opts[:router_uri]}"
router_socket = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::ROUTER, opts[:router_uri], opts)
logger.debug "Setting up from_client_pub_uri socket on #{opts[:from_client_pub_uri]}"
from_client_pub_socket = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PUB, opts[:from_client_pub_uri], opts)
logger.debug "Setting up to_client_pub_uri socket on #{opts[:to_client_pub_uri]}"
to_client_pub_socket = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PUB, opts[:to_client_pub_uri], opts)
logger.debug "Setting up from_sink_uri socket on #{opts[:from_sink_uri]}"
from_sink_socket = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PULL, opts[:from_sink_uri], opts)
logger.debug "Setting up error_uri socket on #{opts[:error_uri]}"
error_socket = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PUSH, opts[:error_uri], opts)

METHODS.each do |method|
  uri = opts["#{method}_uri".to_sym]
  sockets[method] = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PUSH, uri, opts)
end

# We want this router to stop when there's nothing to receive, or when any of
# its push-socket receivers hit their high-water marks.  The pub socket
# should never block regardless.

poller = ZMQ::Poller.new
poller.register_readable(router_socket)
poller.register_readable(from_sink_socket)

running = true

%w(INT TERM KILL).each do | sig |
  Signal.trap(sig) do
    running = false
    Signal.trap(sig, "DEFAULT")
  end
end

while running do
  method = "error"      # by default, all messages will be routed to the "error" sink
  poller.poll_nonblock
  # reading messages that are sent to the router from client
  if poller.readables.include?(router_socket)
    messages = []
    err = router_socket.recv_strings messages
    if err < 0
      logger.error "Error #{err} reading router socket!"
      next
    end

    logger.debug "Read from router socket: #{messages.inspect}"
    # TODO(noah): add routing envelope to Hastur::Envelope and use it here!

    method = process_messages_for_routing(messages)

    # TODO(noah): check envelope and do router acking, if any

    logger.debug "Routing data to #{method.inspect}: #{messages.inspect}"
    logger.debug "Sending to from-client PUB socket"
    err = from_client_pub_socket.send_strings(messages)
    if err < 0
      logger.error "Error #{err} writing to from-client pub socket!"
      next
    end

    if sockets[method.to_sym]
      logger.debug "Pushing packet to #{method} socket"
      err = sockets[method.to_sym].send_strings(messages)
      if err < 0
        logger.error "Error #{err} writing to #{method} socket!"
        next
      end
    else
      logger.error "Pushing packet to error socket due to invalid method #{method}."
      err = error_socket.send_strings(messages)
      if err < 0
        logger.error "Error #{err} writing to error socket with invalid method #{method}!"
        next
      end
    end
    logger.debug "Finished pushing packet to #{method} socket"
  end

  # reading messages that are sent to the router from a sink
  if poller.readables.include?(from_sink_socket)
    messages = []
    err = from_sink_socket.recv_strings(messages)
    if err < 0
      logger.error "Error #{err} reading from sink socket!"
      next
    end

    err = to_client_pub_socket.send_strings messages
    if err < 0
      logger.error "Error #{err} writing to to-client pub socket!"
      next
    end

    logger.debug "Read from sink socket, sending on router socket [#{opts[:router_uri]}]: #{messages.inspect}"
    err = router_socket.send_strings messages
    if err < 0
      logger.error "Error #{err} writing to to-client router socket!"
      next
    end
  end
end
