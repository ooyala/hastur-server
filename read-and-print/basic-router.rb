#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'socket'
require 'trollop'

# TODO(noah):
#   - Add a router envelope?
#   - Check JSON UUID against envelope?
#   - Add real router information in JSON?
#   - More modular code

MultiJson.engine = :yajl

METHODS = [ :register, :notify, :stats, :heartbeat ]

opts = Trollop::options do
  banner <<-EOS
basic-router.rb - a simple 0mq router.  Clients connect to the router URI,
  sinks connect to the sink URIs.

  Options:
EOS
  opt :router_uri, "ZMQ Router (incoming) URI", :default => "tcp://127.0.0.1:4321", :type => String
  opt :from_sink_uri, "ZMQ REQ (incoming from sink) URI", :default => "tcp://127.0.0.1:4323", :
  opt :pub_uri,  "ZMQ Pub URI for sinks", :default => "tcp://127.0.0.1:4322", :type => String

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

def socket_for_type_and_uri(ctx, socket_type, uri, opts = {})
  socket = ctx.socket(ZMQ.const_get("#{socket_type.to_s.upcase}"))

  # These aren't strictly necessary, but the behavior they enable is
  # what we usually expect.  For now, have all sockets use the same
  # options.  Set socket options *before* bind or connect.
  socket.setsockopt(ZMQ::LINGER, opts[:linger]) # flush messages before shutdown
  socket.setsockopt(ZMQ::HWM, opts[:hwm]) # high water mark, the number of buffered messages

  socket.bind uri

  STDERR.puts "New #{socket_type} socket listening on '#{uri}'."

  socket
end

def add_router_envelope(messages)
  hostname = Socket.gethostname

  # TODO(noah): Add more to envelope
  router_envelope = "#{hostname}"

  messages << router_envelope
end

def multi_recv(socket)
  messages = []
  socket.recv_string(data = "")
  messages << data
  while socket.more_parts?
    socket.recv_string(data = "")
    messages << data
  end
  messages
end

def multi_send(socket, messages)
  last_message = messages[-1]

  (messages[0..-2]).each do |message|
    # I know you can't resend a 0mq message...  Does ffi-rzmq shield us from that?
    socket.send_string(message + "", ZMQ::SNDMORE)
  end
  socket.send_string(last_message)
end

sockets = {}
router_socket = socket_for_type_and_uri(ctx, :router, opts[:router_uri], opts)
pub_socket = socket_for_type_and_uri(ctx, :pub, opts[:pub_uri], opts)
error_socket = socket_for_type_and_uri(ctx, :push, opts[:error_uri], opts)

METHODS.each do |method|
  uri = opts["#{method}_uri".to_sym]
  sockets[method] = socket_for_type_and_uri(ctx, :push, uri, opts)
end

# For now, use a simple blocking receive and a simple blocking send.
# We want this router to stop when there's nothing to receive, or when any of
# its push-socket receivers hit their high-water marks.  The pub socket
# should never block regardless.
loop do
  STDERR.puts "Reading from router socket"
  messages = multi_recv(router_socket)
  add_router_envelope(messages)
  STDERR.puts "Routing data: #{messages.inspect}"

  hash = MultiJson.decode(messages[-1]) rescue nil

  if hash
    method = hash['method'] rescue nil

    hash[:router_host] = Socket.gethostname
    data = nil
    data = MultiJson.encode(hash) rescue nil
  end

  STDERR.puts "Sending to PUB socket"
  multi_send(pub_socket, messages)

  if hash && data
    if sockets[hash['method'].to_sym]
      STDERR.puts "Pushing JSON on #{hash['method']} socket"
      multi_send(sockets[hash['method'].to_sym], messages)
    else
      STDERR.puts "Pushing valid JSON on error socket due to method #{hash['method']}."
      multi_send(error_socket, messages)
    end
  else
    # Parse error, forward old data straight to error sink
    STDERR.puts "Sending unparseable JSON to error socket"
    multi_send(error_socket, messages)
  end
end
