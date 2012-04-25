#!/usr/bin/env ruby

require "ffi-rzmq"
require "yajl"
require "multi_json"
require "termite"
require 'trollop'

require "hastur-server/message"
require "hastur-server/service/core-router"
require "hastur-server/service/cassandra-sink"

Ecology.read("hastur-core.ecology")
MultiJson.use :yajl

opts = Trollop::options do
  banner <<-EOS
hastur-core.rb - hastur router & sink combined

  Options:
EOS
  opt :uuid,      "Router UUID",                    :type => :string
  opt :router,    "Router (agent) URI    (ROUTER)", :default => "tcp://*:8126"
  opt :return,    "Direct routing URI      (PULL)", :default => "tcp://*:8127"
  opt :firehose,  "Agent event URI          (PUB)", :default => "tcp://*:8128"
  opt :pidfile,   "Location of pidfile",            :type => :string
  opt :cassandra, "Cassandra server list", :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
end

logger = Termite::Logger.new
ctx = ZMQ::Context.new

uris = {
  :agent_router    => "tcp://*:8126", # router incoming messages from agents
  :agent_return    => "tcp://*:8127", # messages going back to the agents, e.g. acks
  :agent_firehose  => "tcp://*:8128", # all messages from agents, a.k.a. "firehose"
}

router = Hastur::Service::CoreRouter.new(
  opts[:uuid],
  :router_uri   => opts[:router],
  :return_uri   => opts[:return],
  :firehose_uri => opts[:firehose]
)

sink = Hastur::Service::CassandraSink.new(
  :ack_uri   => opts[:return],
  :data_uri  => opts[:firehose],
  :keyspace  => 'Hastur',
  :cassandra => opts[:cassandra],
  :socktype  => ZMQ::SUB
)

# must subscribe to empty string to get everything
sink.subscribe ""

if opts[:pidfile]
  File.open(opts[:pidfile], "w+") { |file| file.puts Process.pid }
end

router_thread = Termite::Thread.new logger do
  router.run
end

sink_thread = Termite::Thread.new logger do
  sink.run
end

# set up signal handlers and hope to be able to get a clean shutdown
%w(INT TERM KILL).each do |sig|
  Signal.trap(sig) do
    router.stop
    sink.stop
    Signal.trap(sig, "DEFAULT")
  end
end

router_thread.join
sink_thread.join

if opts[:pidfile]
  File.unlink(opts[:pidfile]) if File.exists?(opts[:pidfile])
end
