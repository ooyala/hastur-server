#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'trollop'

opts = Trollop::options do
  banner <<-EOS
basic-router.rb - a simple 0mq router.  Clients connect to the router URI,
  servers connect to the push URI.

  Options:
EOS
  opt :router_uri, "ZMQ Router URI", :default => "tcp://127.0.0.1:4321", :type => String
  opt :push_uri, "ZMQ Push URI", :default => "tcp://127.0.0.1:4322", :type => String
  opt :pub_uri,  "ZMQ Pub URI", :default => "tcp://127.0.0.1:4323", :type => String
  opt :linger,  "set ZMQ_LINGER",   :default => 1,                 :type => Integer
  opt :hwm,     "set ZMQ_HWM",      :default => 1,                 :type => Integer
  opt :timeout, "poll timeout",     :default => 0.1
end

[:router_uri, :push_uri, :pub_uri].each do |opt|
  if opts[opt] !~ /\w+:\/\/[^:]+:\d+/
    raise "Option --#{opt} must be of the form protocol://hostname:port rather than #{opts[opt]}"
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

sockets = {}
[:router, :push, :pub].each do |socket_type|
  sockets[socket_type] = ctx.socket(const_get("ZMQ::#{socket_type.upcase}"))
  address = opts["#{socket_type}_uri".to_sym]
  sockets[socket_type].bind address

  STDERR.puts "#{socket_type} socket listening on '#{address}'."

  # these aren't strictly necessary, but the behavior they enable is what we usually expect
  # for now, have router and push sockets both use the same options

  sockets[socket_type].setsockopt(ZMQ::LINGER, opts[:linger]) # flush messages before shutdown
  sockets[socket_type].setsockopt(ZMQ::HWM, opts[:hwm]) # high water mark, the number of buffered messages
end

# For now, use a simple blocking receive and a simple blocking send.
# We want this router to stop when there's nothing to receive, or when any of
# its push-socket receivers hit their high-water marks.  The pub socket
# should never block regardless.
loop do
  data = ""
  socket[:router].recv_string(data)
  STDERR.puts "Routing data: #{data}"

  socket[:pub].send_string data
  socket[:push].send_string data
end
