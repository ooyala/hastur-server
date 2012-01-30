#!/usr/bin/env ruby

require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'
require 'rubygems'
require_relative '../tools/zmq_utils'

def main(routers)
  ctx = ZMQ::Context.new
  router = ctx.socket(ZMQ::PULL)

  routers.each do |router_uri|
    router.connect(router_uri)
  end

  router.setsockopt(ZMQ::HWM, 100)
  router.setsockopt(ZMQ::LINGER, 1)

  loop do
    payload = multi_recv(router)[-1]

    puts payload
  end
end

if __FILE__ == $0
  MultiJson.engine = :yajl

  opts = Trollop::options do
    opt :router, "Router URI", :type => String, :required => true, :multi => true
  end

  main(opts[:router])
end

