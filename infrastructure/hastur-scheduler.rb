#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'

require_relative "../lib/hastur/zmq_utils"

MultiJson.engine = :yajl

# default is: connect('tcp://localhost:5000') as ZMQ::SUB
opts = Trollop::options do
  banner <<-EOS
ScheduleD, which sends plugin notifications back to the client.  Right now, a fake.
EOS
  opt :router,    "ZeroMQ URI",         :default => "tcp://127.0.0.1:4323", :type => String
  opt :linger,    "set ZMQ_LINGER",     :default => 1,                      :type => Integer
  opt :hwm,       "set ZMQ_HWM",        :default => 1,                      :type => Integer
  opt :prefix,    "prefix string",      :default => "scheduleD",            :type => String
  opt :client,    "Client UUID",                                            :type => String, :multi => true, :required => true
  opt :interval,  "Delay between reqs", :default => 45,                     :type => :int
end

if opts[:router] !~ /\w+:\/\/[^:]+:\d+/
  Trollop::die :router, "Option --router must be of the form protocol://hostname:port"
end

if ZMQ::LibZMQ.version2? && opts[:router] =~ /\Wlocalhost\W/
  Trollop::die :router, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
end

ctx = ZMQ::Context.new(1)
router_socket = Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PUSH, opts[:router], opts.merge({ :connect => true }) )

loop do
  opts[:client].each do |uuid|
    hash = {
      "uuid" => uuid,
      "method" => "schedule",
      "plugin" => "fake plugin",
      "plugin_path" => "echo",
      "plugin_args" => "OK",
      "interval" => "#{opts[:interval]} seconds"
    }
    json = MultiJson.encode hash
    Hastur::ZMQUtils.multi_send router_socket, [ uuid, "schedule", json ]
  end

  sleep opts[:interval]
end
