#!/usr/bin/env ruby
require "trollop"
require "hastur-server/zmq_utils"
require "hastur-server/message"
require "hastur-server/sink/cassandra_schema"

opts = Trollop::options do
  banner "Creates a Cassandra keyspace\n\nOptions:"
  opt :hosts,     "Cassandra URI(s)",       :default => ["127.0.0.1:9160"],        :type => :strings,
                                                                                   :multi => true
  opt :routers,   "Router URI(s)",          :default => ["tcp://127.0.0.1"],       :type => :strings,
                                                                                   :multi => true
  opt :sink_port, "Router sink port num(s)",:default => 8127,                      :type => :integer,
                                                                                   :multi => true
  opt :ack_port,  "Router ack port num",    :default => 8134,                      :type => :integer
  opt :keyspace,  "Keyspace",               :default => "Hastur",                  :type => String
  opt :hwm,       "ZMQ message queue size", :default => 1,                         :type => :int
end

ctx = ZMQ::Context.new

# Build a list of URIs that are the direct port on each router
msg_uri_list = opts[:routers].flatten.map { |r| "#{r}:#{opts[:ack_port]}" }

# Build a list of URIs of routers cross sink ports.
# So if there are six routers and we're being a sink for four message types,
#   you'd get 24 URIs to connect to.  Load balancing!
ack_uri_list = opts[:routers].flatten.map { |r| [opts[:sink_port]].flatten.map { |p| "#{r}:#{p}" }}.flatten

msg_socket = Hastur::ZMQUtils.connect_socket(ctx, ::ZMQ::PULL, msg_uri_list)
ack_socket = Hastur::ZMQUtils.connect_socket(ctx, ::ZMQ::PUSH, ack_uri_list)

puts "Connecting to Cassandra at #{opts[:hosts].inspect}"
client = Cassandra.new(opts[:keyspace], opts[:hosts])
client.default_write_consistency = 2  # Initial default: 1

@running = true
%w(INT TERM KILL).each do | sig |
  Signal.trap(sig) do
    @running = false
    Signal.trap(sig, "DEFAULT")
  end
end

while @running do
  begin
    puts "Receiving..."
    message = Hastur::Message.recv(msg_socket)
    envelope = message.envelope
    uuid = message.envelope.from
    route = message.type_symbol.to_s
    puts "[#{route}] - #{message.payload}"
    Hastur::Cassandra.insert(client, message.payload, route, :uuid => uuid)
    envelope.to_ack.send(ack_socket) if envelope.ack?
  rescue Exception => e
    puts e.message
    puts e.backtrace
  end
end

STDERR.puts "Exited!"
