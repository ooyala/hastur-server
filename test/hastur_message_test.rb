#!/usr/bin/env ruby

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require_relative '../lib/hastur/stat'
require_relative '../lib/hastur/message'

class TestClassHasturMessage < MiniTest::Unit::TestCase
  # this should be consistent .... if another json encoder changes the order it will break
  STAT = {
    :name      => "foo.bar",
    :value     => 1024,
    :units     => "s",
    :timestamp => 1328176249.1028926,
    :tags      => { :blahblah => 456 }
  }
  STAT_JSON = '{"name":"foo.bar","value":1024,"units":"s","timestamp":1328176249.1028926,"tags":{"blahblah":456}}'
  STAT_OBJECT = Hastur::Stat.new(STAT)

  DEALER_COUNT = 40 # number of emulated ZMQ::DEALER clients to run
  ROUTER_COUNT = 4  # number of emulated routers to run
  PULLER_COUNT = 4  # number of sinks to run, must divide evenly into dealer count
  DEALER_MESSAGES = DEALER_COUNT * DEALER_COUNT
  PULLER_MESSAGES = DEALER_MESSAGES / PULLER_COUNT # pullers use this to know when to exit, be careful
  DEALER_IDS = 1.upto(DEALER_COUNT).map { SecureRandom.uuid }

  puts "MESSAGES: #{DEALER_MESSAGES}, PULLER_MESSAGES: #{PULLER_MESSAGES}"

  def test_envelope
    uuid = SecureRandom.uuid
    e = Hastur::Envelope.new(
      :route     => :rawdata,
      :uuid      => uuid,
      :timestmap => 1328301436.9485276,
      :uptime    => 12.401439189910889,
      :sequence  => 1234
    )
    assert_equal false, e.ack? # should default to false
    assert_equal :rawdata, e.route

    ehex = e.to_s # returns envelope in hex
    assert_equal "0001",               ehex[0,  4 ], "check version"
    assert_equal "72617764617461",     ehex[4,  14], "check route"
    assert_equal uuid.split(/-/).join, ehex[36, 32], "check uuid"

    assert_raises ArgumentError do
      Hastur::Envelope.new
      Hastur::Envelope.new :foobar
    end

    # :route and :uuid are both required
    assert_raises ArgumentError do
      Hastur::Envelope.new :uuid => SecureRandom.uuid
      Hastur::Envelope.new :route => :error
    end

    # test mispeled ruotes
    assert_raises ArgumentError do
      Hastur::Envelope.new :route => :stats, :uuid => SecureRandom.uuid
      Hastur::Envelope.new :route => :sta,   :uuid => SecureRandom.uuid
      Hastur::Envelope.new :ruote => :stat,  :uuid => SecureRandom.uuid
    end

    acked = Hastur::Envelope.new :route => :stat, :uuid => SecureRandom.uuid.split(/-/).join, :ack => true
    assert_equal true,  acked.ack?
    assert_equal :stat, acked.route

    noack = Hastur::Envelope.new :route => :stat, :uuid => SecureRandom.uuid, :ack => false
    assert_equal false, noack.ack?
    assert_equal :stat, noack.route
    assert_equal 118,   noack.to_s.length
    assert_equal 59,    noack.pack.bytesize
  end

  def test_base
    assert_raises ArgumentError do
      Hastur::Message::Base.new()
      Hastur::Message::Base.new(1)
      Hastur::Message::Base.new(1, 2)
    end
  end

  def test_stat
    e = Hastur::Envelope.new :route => :stat, :uuid => SecureRandom.uuid
    hmsg = Hastur::Message::Stat.new :envelope => e, :stat => STAT
    refute_nil hmsg
    assert_kind_of Hastur::Message::Base, hmsg
    refute_nil hmsg.to_s
    refute_nil hmsg.payload

    assert_equal STAT_JSON, hmsg.payload
  end

  # This test is complicated on purpose. The idea is to over-parallelize with minimal locking
  # shake out concurrency bugs. It has already exposed a few interesting bugs that are now fixed.

  def zmq_sockopts(s)
    s.setsockopt(ZMQ::LINGER, -1)
    s.setsockopt(ZMQ::SNDHWM, DEALER_MESSAGES)
    s.setsockopt(ZMQ::RCVHWM, DEALER_MESSAGES)
  end

  def zmq_router(ctx, router_uri, pusher_uri)
    puts "Router on #{router_uri}"
    puts "Pusher on #{pusher_uri}"

    router = ctx.socket(ZMQ::ROUTER)
    zmq_sockopts(router)
    router.bind(router_uri)

    push = ctx.socket(ZMQ::PUSH)
    zmq_sockopts(push)
    push.bind(pusher_uri)

    saw = 0

    loop do
      rc = router.recvmsgs stuff = [], ZMQ::DONTWAIT
      if rc >= 0
        STDERR.write '<'
        push.sendmsgs stuff
        STDERR.write '>'
        saw += 1
      else
        STDERR.write '-'
        sleep 0.01
        break if @count == DEALER_MESSAGES
      end
    end

    STDERR.write ' P$ R$ '
    STDERR.write "Router(#{saw})"

    push.close
    router.close
  end

  def zmq_puller(ctx, pushers, id)
    sleep 1
    pull = ctx.socket(ZMQ::PULL)
    zmq_sockopts(pull)
    pushers.each { |uri| pull.connect(uri) }

    STDERR.write ' p^ '

    1.upto(PULLER_MESSAGES) do |n|
      #got = Hastur::Message.recv(pull)
      rc = router.recvmsgs stuff = [], ZMQ::DONTWAIT
      STDERR.write '#'
      STDERR.write " (#{n}/#{DEALER_MESSAGES}/#{id}) "
    end

    @mutex.synchronize { @count = @count + PULLER_MESSAGES }

    STDERR.write " p$ (#{@count}/#{DEALER_MESSAGES}/#{id}) "
    pull.close
    puts "\nDONE!\n"
  end

  def zmq_dealer(ctx, uuid, routers)
    sleep 1
    dealer = ctx.socket(ZMQ::DEALER)
    zmq_sockopts(dealer)
    routers.each { |r| dealer.connect(r) }

    STDERR.write ' d^ '

    1.upto(DEALER_COUNT) do |num|
      msg = Hastur::Message::Stat.new(
       :uuid => uuid,
        :stat => {
          :name      => "foo.bar",
          :value     => num,
          :units     => "s",
          :timestamp => Time.new.to_f,
        }
      )
      STDERR.write '@'
      msg.send(dealer)
    end

    dealer.close
    STDERR.write ' d$ '
  end

  def test_zmq_send
    @mutex  = Mutex.new
    ctx     = ZMQ::Context.new
    count   = 0
    threads = []
    routers = []
    dealers = []
    pushers = []

    # start up routers
    1.upto(ROUTER_COUNT) do |num|
      routers << router = "ipc://router#{num}"
      pushers << pusher = "ipc://push#{num}"
      #routers << router = "tcp://127.0.0.1:#{num + 3000}"
      #pushers << pusher = "tcp://127.0.0.1:#{num + 2000}"
      threads << Thread.new { zmq_router(ctx, router, pusher) }
    end

    sleep 1

    # start dealers and produce data (clients)
    1.upto(DEALER_COUNT) do |c|
      uuid = DEALER_IDS[c - 1]
      threads << Thread.new { zmq_dealer(ctx, uuid, routers) }
    end

    sleep 1

    # start up consumers (sinks)
    1.upto(PULLER_COUNT) do |n|
      threads << Thread.new { zmq_puller(ctx, pushers, n) }
    end

    STDERR.write ' $$ '
    stopped = 0
    threads.each {|t| t.join } #; stopped += 1; puts "#{stopped}/#{threads.count} stopped"}
    ctx.terminate
  end

  # below are dumb placeholders for the moment

  def test_error
    err = Hastur::Message::Error.new :error => "eek!", :uuid => SecureRandom.uuid
    assert_kind_of Hastur::Message::Base, err
  end
  def test_rawdata
    Hastur::Message::Rawdata
  end
  def test_plugin_exec
    Hastur::Message::PluginExec
  end
  def test_plugin_result
    Hastur::Message::PluginResult
  end
  def test_register_client
    Hastur::Message::RegisterClient
  end
  def test_register_service
    Hastur::Message::RegisterService
  end
  def test_register_plugin
    Hastur::Message::RegisterPlugin
  end
end

