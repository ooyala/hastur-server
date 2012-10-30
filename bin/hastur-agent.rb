#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'socket'
require 'termite'

require "hastur/api"
require "hastur-server/util"
require "hastur-server/service/agent"

MultiJson.use :yajl
NOTIFICATION_INTERVAL = 5   # Hardcode for now

opts = Trollop::options do
  opt :router,      "Router URI",         :type => String, :default => "tcp://127.0.0.1:8126", :multi => true
  opt :uuid,        "System UUID",        :type => String, :required => true
  opt :port,        "Local socket port",  :default => 8125
  opt :unix,        "UNIX domain socket", :type => String
  opt :heartbeat,   "Heartbeat interval", :default => 30
  opt :ohai_info,   "Ohai information interval", :default => 3600
  opt :agent_reg,   "Agent registration interval", :default => 3600
  opt :ack_timeout, "Time between unacked message resends", :default => 10
  opt :pidfile,     "Location of pidfile", :type => String
  opt :debug,       "Enable debug logging", :default => false
  opt :no_agent_stats, "disable sending of agent stats, mostly for tests"
  opt :no_proc_stats, "disable sending of process stats, mostly for tests"
end

unless opts[:router].all? { |uri| Hastur::Util.valid_zmq_uri? uri }
  Trollop::die :router, "must be in this format: protocol://hostname:port"
end

opts[:routers] = opts[:router]
opts[:port] = opts[:port].to_i
opts[:logger] = Termite::Logger.new

if opts[:debug]
  opts[:logger].level = Logger::DEBUG
else
  opts[:logger].level = Logger::INFO
end

agent = Hastur::Service::Agent.new(opts)

if opts[:pidfile]
  File.open(opts[:pidfile], "w") { |file| file.puts Process.pid }
end

%w(INT TERM KILL).each do | sig |
  Signal.trap(sig) do
    agent.stop
    Signal.trap(sig, "DEFAULT")
  end
end

opts[:logger].debug "calling run ..."

agent.setup
agent.run
agent.shutdown

opts[:logger].debug "run exited ..."

if opts[:pidfile]
  File.unlink(opts[:pidfile]) if File.exists?(opts[:pidfile])
end
