#!/usr/bin/env ruby

require "ffi-rzmq"
require "yajl"
require "multi_json"
require "termite"
require 'trollop'

require "hastur-server/message"
require "hastur-server/service/sink"

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
  opt :pidfile,   "Location of pidfile",            :type => :string
  opt :debug,     "Enable debug logging",           :default => false
  opt :cassandra, "Cassandra server list", :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
end

logger = Termite::Logger.new
ctx = ZMQ::Context.new

if opts[:debug]
  logger.level = Logger::DEBUG
end

if opts[:pidfile]
  File.open(opts[:pidfile], "w+") { |file| file.puts Process.pid }
end

client = nil
if RUBY_PLATFORM == "java"
  require "hastur-server/api/cass_java_client"
  client = ::Hastur::API::CassandraJavaClient.new opts[:cassandra].flatten
end

sink = Hastur::Service::Sink.new(opts[:uuid],
  :client       => client,
  :logger       => opts[:logger],
  :router_uri   => opts[:router],
  :cassandra => opts[:cassandra].flatten,
  :keyspace  => 'hastur',
)

sink.setup

# set up signal handlers and hope to be able to get a clean shutdown
%w(INT TERM KILL).each do |sig|
  Signal.trap(sig) do
    sink.stop
    Signal.trap(sig, "DEFAULT")
  end
end

sink.run
sink.shutdown

if opts[:pidfile]
  File.unlink(opts[:pidfile]) if File.exists?(opts[:pidfile])
end
