#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'
require 'socket'
require 'termite'

require "hastur-server/util"
require "hastur-server/service/agent"

MultiJson.use :yajl
NOTIFICATION_INTERVAL = 5   # Hardcode for now
UUID_FILE = "/etc/uuid" # Default location of the system's UUID

opts = Trollop::options do
  opt :router,      "Router URI",         :type => String, :default => "tcp://127.0.0.1:8126", :multi => true
  opt :uuid,        "System UUID",        :type => String
  opt :port,        "Local socket port",  :default => 8125
  opt :unix,        "UNIX domain socket", :type => String
  opt :heartbeat,   "Heartbeat interval", :default => 30
  opt :ack_timeout, "Time between unacked message resends", :default => 10
  opt :pidfile,     "Location of pidfile", :type => String
end

unless opts[:router].all? { |uri| Hastur::Util.valid_zmq_uri? uri }
  Trollop::die :router, "must be in this format: protocol://hostname:port"
end

unless opts[:uuid]
  if File.readable?(UUID_FILE) and File.size(UUID_FILE) == 37
    opts[:uuid] = File.read(UUID_FILE).chomp
  else
    opts[:uuid] = UUID.new.generate
    if File.writable?(UUID_FILE) or File.writable?(File.dirname(UUID_FILE))
      File.open(UUID_FILE, "w+") { |file| file.puts opts[:uuid] }
    end
  end
end

opts[:routers] = opts[:router]
opts[:port] = opts[:port].to_i

agent = Hastur::Service::Agent.new(opts)

if opts[:pidfile]
  File.open(opts[:pidfile], "w+") { |file| file.puts Process.pid }
end

%w(INT TERM KILL).each do | sig |
  Signal.trap(sig) do
    agent.shutdown
    Signal.trap(sig, "DEFAULT")
  end
end

agent.run

if opts[:pidfile]
  File.unlink(opts[:pidfile]) if File.exists?(opts[:pidfile])
end
