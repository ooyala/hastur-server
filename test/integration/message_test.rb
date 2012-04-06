#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'hastur-server/message'
require 'hastur-server/util'

# This test is complicated on purpose. The idea is to over-parallelize with minimal locking
# shake out concurrency bugs. It has already exposed a few interesting bugs that are now fixed.

class TestClassHasturMessageIntegration < MiniTest::Unit::TestCase
  DEALER_COUNT = 20 # number of emulated ZMQ::DEALER agents to run
  ROUTER_COUNT = 2  # number of emulated routers to run
  PULLER_COUNT = 4  # number of sinks to run, must divide evenly into dealer count
  DEALER_MESSAGES = DEALER_COUNT * DEALER_COUNT
  PULLER_MESSAGES = DEALER_MESSAGES / PULLER_COUNT # pullers use this to know when to exit, be careful
  ROUTER_MESSAGES = DEALER_MESSAGES / ROUTER_COUNT # pullers use this to know when to exit, be careful
  DEALER_IDS = DEALER_COUNT.times.map { SecureRandom.uuid }

  puts "MESSAGES: #{DEALER_MESSAGES}, PULLER_MESSAGES: #{PULLER_MESSAGES}"

  def zmq_sockopts(s)
    s.setsockopt(ZMQ::LINGER, -1)
    if ZMQ::LibZMQ.version2?
      s.setsockopt(ZMQ::HWM, 10)
    elsif  ZMQ::LibZMQ.version3?
      s.setsockopt(ZMQ::SNDHWM, 10)
      s.setsockopt(ZMQ::RCVHWM, 10)
    end
  end

  def zmq_router(ctx, router_uri, pusher_uri)
    router = ctx.socket(ZMQ::ROUTER)
    zmq_sockopts(router)
    router.bind(router_uri)
    STDERR.write " R^ "

    push = ctx.socket(ZMQ::PUSH)
    zmq_sockopts(push)
    push.bind(pusher_uri)
    STDERR.write " P^ "

    count = 0
    loop do
      msg = Hastur::Message.recv(router)
      msg.send(push)
      count += 1
      STDERR.write '-'
      break if count == ROUTER_MESSAGES
    end

    sleep 2

    push.close
    STDERR.write ' P$ '
    router.close
    STDERR.write ' R$ '
  end

  def zmq_puller(ctx, pushers, id)
    pull = ctx.socket(ZMQ::PULL)
    zmq_sockopts(pull)
    pushers.each { |uri| pull.connect(uri) }

    STDERR.write ' p^ '

    count = 0
    loop do
      msg = Hastur::Message.recv(pull)
      count += 1
      break if count == PULLER_MESSAGES
      STDERR.write "<"
    end

    pull.close
    STDERR.write "p$"
  end

  def zmq_dealer(ctx, uuid, count, routers)
    dealer = ctx.socket(ZMQ::DEALER)
    zmq_sockopts(dealer)
    routers.each { |r| dealer.connect(r) }

    STDERR.write ' d^ '

    count.times do |num|
      msg = Hastur::Message::Stat::Gauge.new(
        :from => uuid,
        :data => {
          :name      => "foo.bar",
          :type      => "gauge",
          :value     => num,
          :timestamp => Hastur::Util.timestamp,
          :labels    => {},
        }
      )
      #puts MultiJson.encode(msg.to_hash)
      msg.send(dealer)
      STDERR.write '>'
    end

    dealer.close
    STDERR.write ' d$ '
  end

  def test_zmq_send
    ctx     = ZMQ::Context.new
    @sent   = 0
    @routed = 0
    @sunk   = 0
    router_uris = []
    pusher_uris = []
    routers = {}
    dealers = {}
    pullers = {}

    # start up routers
    ROUTER_COUNT.times do |num|
      router_uris << router = "ipc://router#{num}"
      pusher_uris << pusher = "ipc://push#{num}"
      routers[router] = Thread.new do
        zmq_router(ctx, router, pusher) rescue STDERR.puts $!.inspect, $@
      end
    end

    sleep 2

    # start up consumers (sinks)
    PULLER_COUNT.times do |num|
      pullers[num] = Thread.new do
        zmq_puller(ctx, pusher_uris, "puller#{num}") rescue STDERR.puts $!.inspect, $@
      end
    end

    sleep 2

    # start dealers and produce data (agents)
    DEALER_COUNT.times do |num|
      uuid = DEALER_IDS[num - 1]
      dealers[uuid] = Thread.new do 
        zmq_dealer(ctx, uuid, DEALER_COUNT, router_uris) rescue STDERR.puts $!.inspect, $@
      end
    end

    # block until all the threads are done
    dealers.each {|_,thr| thr.join }
    pullers.each {|_,thr| thr.join }
    routers.each {|_,thr| thr.join }

    STDERR.write ' $$ '
  end
end

