#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'
require 'socket'
require 'termite'

require "uuid"
require "hastur-server/zmq_utils"
require "hastur-server/util"
require "hastur-server/client/service"

MultiJson.engine = :yajl
NOTIFICATION_INTERVAL = 5   # Hardcode for now
UUID_FILE = "/etc/uuid" # Default location of the system's UUID

opts = Trollop::options do
  opt :router,      "Router URI",         :type => String, :default => "tcp://*:8126", :multi => true
  opt :uuid,        "System UUID",        :type => String
  opt :port,        "Local socket port",  :default => 8125
  opt :unix,        "UNIX domain socket", :type => String
  opt :heartbeat,   "Heartbeat interval", :default => 30
  opt :ack_timeout, "Time between unacked message resends", :default => 10
end

unless opts[:router].any? { |uri| Hastur::Util.valid_zmq_uri? uri }
  Trollop::die :router, "must be in this format: protocol://hostname:port"
end

unless opts[:uuid]
  if File.readable? UUID_FILE
    opts[:uuid] = File.read(UUID_FILE).chomp
  else
    opts[:uuid] = UUID.new.generate
    if File.writable?(UUID_FILE) or File.writable?(File.dirname(UUID_FILE))
      File.open(UUID_FILE, "w") { |file| file.puts uuid }
    end
  end
end

opts[:routers] = opts[:router]
opts[:port] = opts[:port].to_i

client = Hastur::Client::Service.new(opts)
client.run
