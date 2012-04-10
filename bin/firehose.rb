#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

#
# A temporary firehose script to merge all the routers into a single
# firehose PUB socket.  This will be replaced with the real syndicators.
#

require 'ffi-rzmq'
require 'trollop'

require "hastur-server/util"

opts = Trollop::options do
  banner <<-EOS
firehose.rb - a temporary solution for pulling all routers into a single firehose

  Options:
EOS
  opt :router,       "Upsream router URI list (PULL)", :type => :strings, :multi => true, :required => true
  opt :firehose,     "Upsream router URI list  (PUB)", :default => "tcp://*:9136"
end

UUID = "3824977c-fd6b-442d-a71f-ebb7cbf5a1ec"

ctx = ZMQ::Context.new
routers = ctx.socket ZMQ::PULL
firehose = ctx.socket ZMQ::PUB

Hastur::Util.setsockopts routers, :hwm => 1_000, :linger => 5
Hastur::Util.setsockopts firehose, :hwm => 1_000, :linger => 5

opts[:router].flatten.each do |uri|
  routers.connect uri
end
firehose.bind opts[:firehose]

errors = 0
forwarded = 0
loop do
  rc = routers.recvmsgs messages=[]
  if ZMQ::Util.resultcode_ok? rc
    rc = firehose.sendmsgs messages
    if ZMQ::Util.resultcode_ok? rc
      forwarded += 1
    else
      errors += 1
    end
  else
    errors += 1
  end
  messages.each { |m| m.close rescue nil }
end

routers.close
firehose.close
ctx.terminate
