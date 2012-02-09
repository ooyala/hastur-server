#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'
require 'socket'
require 'termite'

require "hastur/zmq_utils"
require "hastur/client/uuid"
require "hastur/plugin/v1"
require "hastur/client"

MultiJson.engine = :yajl
NOTIFICATION_INTERVAL = 5   # Hardcode for now

opts = Trollop::options do
  opt :router,    "Router URI",         :type => String,  :required => true, :multi => true
  opt :uuid,      "System UUID",        :type => String
  opt :port,      "Local socket port",  :type => String,  :required => true
  opt :heartbeat, "Heartbeat interval", :type => Integer, :default => 15
end

if opts[:router].any? { |uri| uri !~ /\w+:\/\/[^:]+:\d+/ }
  Trollop::die :router, "--router is required and must be in this format: protocol://hostname:port"
end

unless opts[:uuid]
  # attempt to retrieve UUID from disk; UUID gets created on the fly if it doesn't exist
  opts[:uuid] = Hastur::ClientUtil::UUID.get_uuid
  puts opts[:uuid]
end

opts[:routers] = opts[:router]
opts[:port] = opts[:port].to_i

client = ::Hastur::Client.new(opts)
client.run




