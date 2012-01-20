#!/usr/bin/env ruby

require "rubygems"
require "ffi-rzmq"
require "trollop"
require "pp"

# This is just for using on the command line
SocketMap = {
  "pull" => "PULL",
  "push" => "PUSH",
  "subscribe" => "SUB",
  "sub" => "SUB",
  "publish" => "PUB",
  "pub" => "PUB",
  "request" => "REQ",
  "req" => "REQ",
  "reply" => "REP",
  "rep" => "REP",
  "dealer" => "XREQ",
  "xreq" => "XREQ",
  "router" => "XREP",
  "xrep" => "XREP",
}
SocketTypes = SocketMap.keys

opts = Trollop::options do
  version "Read-and-Print v 0.0.1, (c) 2012 Ooyala, Inc."
  banner <<-EOS
Point this at your router or other data source to suck down packets and print them out over ZMQ.

Usage: #{$0} [options]
where [options] are:
EOS
  opt :socket_type, "Socket type", :default => :pull, :type => String
  opt :target, "Target hostname", :default => "hastur-router1.us-east-1.ooyala.com", :type => String
  opt :target_port, "Target port number", :default => 4515, :type => :int
  opt :operation, "Operation (connect or bind)", :default => "connect", :type => String
end
Trollop::die :socket_type, "must be one of #{SocketTypes.join(',')}!" unless
  SocketTypes.include?(opts[:socket_type])
Trollop::die :target_port, "must be non-negative!" if opts[:target_port] < 0

ctx = ZMQ::Context.new
s = ctx.socket const_get("ZMQ::#{SocketMap[opts[:socket_type]]}")
rc = s.send(opts[:operation], "#{opts[:target_hostname]}:#{opts[:target_port]}")

# Loop forever, waiting
msg = ""
loop do
  rc = s.recv_string msg
  pp msg.inspect
end
