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
SINK_TIMEOUT_SECONDS = 10

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
  opt :debug,     "Enable debug logging",           :default => false
  opt :no_sink,   "Turn off sink, use only router"
  opt :cassandra, "Cassandra server list", :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
end

logger = Termite::Logger.new
ctx = ZMQ::Context.new

if opts[:debug]
  logger.level = Logger::DEBUG
end

uris = {
  :agent_router    => "tcp://*:8126", # router incoming messages from agents
  :agent_return    => "tcp://*:8127", # messages going back to the agents, e.g. acks
  :agent_firehose  => "tcp://*:8128", # all messages from agents, a.k.a. "firehose"
}

router = Hastur::Service::CoreRouter.new(
  opts[:uuid],
  :router_uri   => opts[:router],
  :return_uri   => opts[:return],
  :firehose_uri => opts[:firehose],
  :logger       => opts[:logger],
)

sink = Hastur::Service::CassandraSink.new(
  :ack_uri   => opts[:return],
  :data_uri  => opts[:firehose],
  :keyspace  => 'hastur',
  :cassandra => opts[:cassandra].flatten,
  :socktype  => ZMQ::SUB,
  :logger    => logger,
) unless opts[:no_sink]

if opts[:pidfile]
  File.open(opts[:pidfile], "w+") { |file| file.puts Process.pid }
end

unless opts[:no_sink]
  sink_thread = Termite::Thread.new logger do
    sink.setup

    # must subscribe to empty string to get everything
    sink.subscribe "" unless opts[:no_sink]

    sink.run
    sink.shutdown
  end

  # wait for the sink to come up before proceding to start the router
  # if they come up out of order, there's a higher chance of message loss
  1.upto(SINK_TIMEOUT_SECONDS * 100) do |try|
    sleep 0.1
    if sink.running?
      logger.info "Sink up and running."
      break
    elsif try == SINK_TIMEOUT_SECONDS * 100
      abort "Sink did not come up before timeout."
    end
  end
end

router_thread = Termite::Thread.new logger do
  router.setup
  router.run
  router.shutdown
end

# set up signal handlers and hope to be able to get a clean shutdown
%w(INT TERM KILL).each do |sig|
  Signal.trap(sig) do
    router.stop
    sink.stop unless opts[:no_sink]
    Signal.trap(sig, "DEFAULT")
  end
end

# simple supervisor for the threads: sleep in the main thread until either
# one exits then do a normal shutdown
begin
  sleep 1.0
end while (sink.running? or opts[:no_sink]) and router.running?

# set the run flag to false for a clean shutdown
# shutdown must be called in the running thread, so it is set up above
sink.stop
router.stop

# join the threads so they have a chance to exit cleanly
# might want to come up with some kind of timeout, but it seems to work
# as expected in manual tests
router_thread.join
sink_thread.join unless opts[:no_sink]

if opts[:pidfile]
  File.unlink(opts[:pidfile]) if File.exists?(opts[:pidfile])
end
