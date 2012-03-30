#!/usr/bin/env ruby

# e.g. ruby hastur-core-router.rb --uuid $(uuidgen) -r ipc:///tmp/s1 -i ipc:///tmp/s2 -s ipc:///tmp/s3 -d tcp://127.0.0.1:1234 -c localhost:9160

require "ffi-rzmq"
require "trollop"
require "dcell"
require "dcell/registries/cassandra_adapter"
require "hastur-server/service/core-router"

opts = Trollop::options do
  banner <<-EOS
hastur-core-router.rb - route to/from Hastur clients

  Options:
EOS
  opt :uuid,           "Router UUID (for logging)",      :type => String
  opt :hwm,            "ZeroMQ message queue depth",     :default => 1
  opt :router,         "Router (client) URI   (ROUTER)", :default => "tcp://*:8126"
  opt :incoming,       "All the incoming data   (PUSH)", :default => "tcp://*:8127"
  opt :syndication,    "All the incoming data   (PUSH)", :default => "tcp://*:8128"
  opt :outgoing,       "Direct routing URI      (PULL)", :default => "tcp://*:8129"
  opt :dcell,          "Direct routing URI     (dcell)", :default => "tcp://*:8130"
  opt :cassandra,      "Cassandra address      (dcell)", :default => "localhost:9160"
end

# Register with DCell
DCell.start :id => "core-router-#{opts[:uuid]}", :addr => opts[:dcell], :registry => {
  :adapter      => 'cassandra',
  :keyspace     => 'Hastur',
  :columnfamily => 'dcell',
  :server       => opts[:cassandra]
}

# Let Celluloid supervise it
Hastur::Service::CoreRouter.supervise_as(:core_router,
  opts[:uuid],
  :hwm          => opts[:hwm],
  :router_uri   => opts[:router],
  :incoming_uri => opts[:incoming],
  :outgoing_uri => opts[:outgoing],
)

sleep
