#!/usr/bin/env ruby
# encoding: utf-8
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'rainbow'

opts = Trollop::options do
  banner "router_ctl.rb - control the hastur router"
  opt :uri, "Router control URI", :type => String, :required => true
  opt :dump, "ask the router to dump its routing table"
end

if opts[:dump]
  command = {:method => "route_dump", :id => 0, :params => {}}
end

ctx = ZMQ::Context.new
sock = ctx.socket(ZMQ::REQ)
sock.connect(opts[:uri])

sock.send_string(MultiJson.encode(command))
rc = sock.recv_string json=""
hash = MultiJson.decode(json)
puts MultiJson.encode(hash, :pretty => true)

