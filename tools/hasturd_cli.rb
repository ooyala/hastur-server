#!/usr/bin/env ruby

#
# HasturD CLI is used to mock PUSH/PULL daemons. The actual payload of the message
# sent needs to be fed through the 'input' option. A typical run would look like this:
# 
#   ./hasturd_cli.rb --client myuuid --input tests/schedule.json --method schedule
#

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'

require_relative "zmq_utils"

MultiJson.engine = :yajl

opts = Trollop::options do
  banner <<-EOS
HasturD, which sends information back to the client.  Right now, a fake.
EOS
  opt :router,    "ZeroMQ URI",         :default => "tcp://127.0.0.1:4323", :type => String
  opt :linger,    "set ZMQ_LINGER",     :default => 1,                      :type => Integer
  opt :hwm,       "set ZMQ_HWM",        :default => 1,                      :type => Integer
  # TODO(viet): Figure out what this is used for
  opt :prefix,    "prefix string",      :default => "scheduleD",            :type => String
  opt :client,    "Client UUID",                                            :type => String, :multi => true, :required => true
  opt :interval,  "Delay between reqs", :default => 45,                     :type => Integer
  opt :method,    "Method for client",                                      :type => String, :required => true 
  opt :input,     "JSON to pass to router",                                 :type => String, :required => true
end

if opts[:router] !~ /\w+:\/\/[^:]+:\d+/
  Trollop::die :router, "Option --router must be of the form protocol://hostname:port"
end

if ZMQ::LibZMQ.version2? && opts[:router] =~ /\Wlocalhost\W/
  Trollop::die :router, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
end

ctx = ZMQ::Context.new(1)
router_socket = socket_for_type_and_uri(ctx, :push, opts[:router], opts.merge({ :connect => true }) )

# Retrieve the JSON object from file and place it in a hash
body = ""
File.read(opts[:input]).each_line { |line| body << line }
hash = MultiJson.decode body

loop do
  opts[:client].each do |uuid|
    # Always set the client UUID on every message
    hash[:uuid] = uuid
    json = MultiJson.encode hash
    puts "Attempting to send message..."
    # opts[:method] is the routing key
    multi_send router_socket, [ uuid, opts[:method], json ]
    puts "Sent message #{json.to_s}"
  end

  sleep opts[:interval]
end
