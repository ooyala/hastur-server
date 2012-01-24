#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'socket'
require 'trollop'

require_relative "zmq_utils"

MultiJson.engine = :yajl

METHODS = [ :register, :notify, :stats, :heartbeat, :logs ]

opts = Trollop::options do
  banner <<-EOS
basic-router.rb - a simple 0mq router.  Clients connect to the router URI,
  sinks connect to the sink URIs.

  Options:
EOS
  opt :router_uri, "ZMQ Router (incoming) URI", :default => "tcp://127.0.0.1:4321", :type => String
  opt :pub_uri,  "ZMQ Pub URI for sinks", :default => "tcp://127.0.0.1:4322", :type => String
  opt :from_sink_uri, "ZMQ REQ (incoming from sink) URI", :default => "tcp://127.0.0.1:4323", :type => String

  port = 4330
  METHODS.each do |method|
    opt "#{method}_uri".to_sym, "ZMQ #{method} sink URI",
      :default => "tcp://127.0.0.1:#{port}", :type => String
    port += 1
  end

  opt :error_uri, "ZMQ Error sink URI", :default => "tcp://127.0.0.1:4350", :type => String
  opt :linger,  "set ZMQ_LINGER",   :default => 1,                 :type => Integer
  opt :hwm,     "set ZMQ_HWM",      :default => 1,                 :type => Integer
  opt :timeout, "poll timeout",     :default => 0.1
end

method_uris = METHODS.map(&:to_s).map { |s| s + "_uri" }.map(&:to_sym)

(method_uris + [:router_uri, :pub_uri, :error_uri]).each do |opt|
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
STDERR.puts "Using ZeroMQ version #{version}"

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
router_socket = socket_for_type_and_uri(ctx, :router, opts[:router_uri], opts)
pub_socket = socket_for_type_and_uri(ctx, :pub, opts[:pub_uri], opts)
from_sink_socket = socket_for_type_and_uri(ctx, :rep, opts[:from_sink_uri], opts)
error_socket = socket_for_type_and_uri(ctx, :push, opts[:error_uri], opts)

METHODS.each do |method|
  uri = opts["#{method}_uri".to_sym]
  sockets[method] = socket_for_type_and_uri(ctx, :push, uri, opts)
end

# We want this router to stop when there's nothing to receive, or when any of
# its push-socket receivers hit their high-water marks.  The pub socket
# should never block regardless.

poller = ZMQ::Poller.new
poller.register_readable(router_socket)
poller.register_readable(from_sink_socket)

loop do
  method = "error"

  poller.poll_nonblock
  if poller.readables.include?(router_socket)
    messages = multi_recv(router_socket)
    STDERR.puts "Read from router socket: #{messages.inspect}"
    method = process_messages_for_routing(messages)
    STDERR.puts "Routing data to #{method.inspect}: #{messages.inspect}"

    STDERR.puts "Sending to PUB socket"
    multi_send(pub_socket, messages)

    if sockets[method.to_sym]
      STDERR.puts "Pushing packet to #{method} socket"
      multi_send(sockets[method.to_sym], messages)
    else
      STDERR.puts "Pushing packet to error socket due to invalid method #{method}."
      multi_send(error_socket, messages)
    end
  end

  if poller.readables.include?(from_sink_socket)
    messages = multi_recv(from_sink_socket)
    STDERR.puts "Read from sink socket, sending on router socket: #{messages.inspect}"

    multi_send(router_socket, messages)
    STDERR.puts "Finished sending on router socket"
  end
end
