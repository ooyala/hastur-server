#!/usr/bin/env ruby

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "lib")

require "trollop"
require "hastur/zmq_utils"
require "hastur/message"
require "hastur/sink/cassandra_schema"

opts = Trollop::options do
  banner "Creates a Cassandra keyspace\n\nOptions:"
  opt :hosts,    "Cassandra Hostname(s)",  :default => ["127.0.0.1"],        :type => :strings, :multi => true
  opt :routers,  "Router URI(s)",          :default => ["tcp://127.0.0.1:8127"], :type => :strings, :multi => true
  opt :keyspace, "Keyspace",               :default => "Hastur",           :type => String
  opt :hwm,      "ZMQ message queue size", :default => 1,                  :type => :int
end

puts "Connecting to database at #{opts[:hosts][0]}:9160"
client = Cassandra.new(opts[:keyspace], opts[:hosts].map { |h| "#{h}:9160" })

client.default_write_consistency = 2  # Initial default: 1

socket = Hastur::ZMQUtils.connect_socket(ZMQ::Context.new, ZMQ::PULL, opts[:routers][0])

@running = true

%w(INT TERM KILL).each do | sig |
  Signal.trap(sig) do
    @running = false
    Signal.trap(sig, "DEFAULT")
  end
end

while @running do
  message = Hastur::Message.recv(socket)
  uuid = message.envelope.from
  route = Hastur::ROUTE_NAME[message.envelope.to]
  Hastur::Cassandra.insert(client, message.payload, :uuid => uuid, :route => route)
end

STDERR.puts "Exited!"
