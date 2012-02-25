#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'hastur-server/message'

class TestClassHasturMessage < MiniTest::Unit::TestCase
  UUID = "01234567-89ab-cdef-deaf-cafedeadbeef"

  # this should be consistent .... if another json encoder changes the order it will break
  STAT = {
    :name      => "foo.bar",
    :value     => 1024,
    :timestamp => 1329865874428623,
    :labels    => { :blahblah => 456, :units => "s" }
  }
  STAT_JSON = '{"name":"foo.bar","value":1024,"timestamp":1329865874428623,"labels":{"blahblah":456,"units":"s"}}'

  ENVELOPE = {
    :from      => "be7f4980-6a1f-4120-b0ce-26de709afcf6",
    :route     => :rawdata,
    :timestamp => 1329865874428623,
    :uptime    => 12.400000000000000,
    :sequence  => 88888
  }

  SECRET = "123456"
  HMAC_HEX = "8861a1a7e8e826df5bee09da489f1c129c9b8b9c3ad96fee75f9ee1cc096fe99"
  INPROC_URI = "inproc://test1"

  def test_envelope
    e = Hastur::Envelope.new(
      :route     => :rawdata,
      :from      => UUID,
      :timestamp => 1328301436948527,
      :uptime    => 12.401439189910889,
      :sequence  => 1234
    )
    assert_equal false, e.ack? # should default to false
    assert_equal '72617764-6174-6100-0000-000000000000', e.to

    ehex = e.to_s # returns envelope in hex
    assert_equal "0001",               ehex[0,  4 ], "check version"
    assert_equal "72617764617461",     ehex[4,  14], "check route"
    assert_equal UUID.split(/-/).join, ehex[36, 32], "check uuid"

    assert_raises ArgumentError do
      Hastur::Envelope.new
      Hastur::Envelope.new :foobar
    end

    # :route and :uuid are both required
    assert_raises ArgumentError do
      Hastur::Envelope.new :from  => SecureRandom.uuid
      Hastur::Envelope.new :route => :error
    end

    # test mispeled ruotes
    assert_raises ArgumentError do
      Hastur::Envelope.new :route => :stats, :from => SecureRandom.uuid
      Hastur::Envelope.new :route => :sta,   :from => SecureRandom.uuid
      Hastur::Envelope.new :ruote => :stat,  :from => SecureRandom.uuid
      Hastur::Envelope.new :ruote => :data,  :from => SecureRandom.uuid
      Hastur::Envelope.new :route => :dat,   :from => SecureRandom.uuid
    end

    acked = Hastur::Envelope.new :route => :stat, :from => SecureRandom.uuid.split(/-/).join, :ack => true
    assert_equal true, acked.ack?
    assert_equal '73746174-0000-0000-0000-000000000000', acked.to

    noack = Hastur::Envelope.new :route => :stat, :from => SecureRandom.uuid, :ack => false
    assert_equal false, noack.ack?
    assert_equal 184,   noack.to_s.length
    assert_equal 92,    noack.pack.bytesize
    assert_equal '73746174-0000-0000-0000-000000000000', noack.to
  end

  def test_router_trace
    r1uuid = SecureRandom.uuid
    r2uuid = SecureRandom.uuid

    e = Hastur::Envelope.new(ENVELOPE)

    # do the first check to make sure its sane, further checks should grow by 32 bytes
    len = e.to_s.length
    assert_equal len, e.to_s.length, "adding a router should grow the hex representation by exactly 32 bytes"

    e.add_router r1uuid
    assert_equal (len + 32), (e.to_s.length), "adding a router should grow the hex representation by exactly 32 bytes"

    e.add_router r2uuid
    assert_equal (len + 64), (e.to_s.length), "adding a router should grow the hex representation by exactly 32 bytes"
  end

  def test_hmac
    secret = "abc123"
    data = "abcdefghijklmnopqrstuvwxyz0123456789"
    hmac = 'e373cf87ca888d48230c1a2dc0bd58a5c6167f8c668dcbf7983e6fcbd9a67e6d'

    e = Hastur::Envelope.new(ENVELOPE)

    assert_equal '', e.hmac, "hmac should be empty on a newly-created envelope"

    hmhex = e.update_hmac(data, secret)
    assert_equal hmac, hmhex, "hmac should match static data"
  end

  def test_serialize
    zmq_part = "00670243f680d448deae3f2ca4513bb1e8"
    e = Hastur::Message::Rawdata.new(ENVELOPE.merge(:payload => "a b c d e f g"))

    json1= "{\"klass\":\"Hastur::Message::Rawdata\",\"envelope\":{\"version\":1,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"be7f4980-6a1f-4120-b0ce-26de709afcf6\",\"ack\":0,\"sequence\":88888,\"timestamp\":1329865874428623,\"uptime\":12.4},\"payload\":\"a b c d e f g\",\"zmq_parts\":[]}"
    json2 = "{\"klass\":\"Hastur::Message::Log\",\"envelope\":{\"version\":1,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"ac8084af-955f-4a48-ac8d-6d2c73f33a75\",\"ack\":1,\"sequence\":1234,\"timestamp\":1328301436000000,\"uptime\":15.9},\"payload\":\" 90ea9aa814354df5a4e82921c63a42cb \",\"zmq_parts\":[\"#{zmq_part}\"]}"

    assert_equal json1, e.to_json, "converting envelope to json matches static data"

    e1 = Hastur::Message::Rawdata.from_json(json1)
    refute_nil e1
    assert e1.zmq_parts.empty?, "First json string has no zmq parts."

    e2 = Hastur::Message::Log.from_json(json2)
    refute_nil e2
    refute e2.zmq_parts.empty?, "Second json string has a zmq part."
    assert_kind_of ZMQ::Message, e2.zmq_parts[0], "ZMQ part decoded correctly."

    assert_equal json1, e1.to_json, "re-encode decoded message should be exact same json"
    assert_equal json2, e2.to_json, "re-encode decoded message should be exact same json"
  end

  def test_over_zmq
    ctx = ZMQ::Context.new

    thr = Thread.new do
      ssock = ctx.socket(ZMQ::PAIR)
      ssock.connect(INPROC_URI)

      e = Hastur::Envelope.new(ENVELOPE)
      m = Hastur::Message::Rawdata.new(:envelope => e, :payload => "a b c d e f g")

      m.send(ssock, :secret => SECRET)
      ssock.close
    end

    rsock = ctx.socket(ZMQ::PAIR)
    rsock.bind(INPROC_URI)

    msg = Hastur::Message.recv(rsock)
    assert_kind_of Hastur::Message::Rawdata, msg
    assert_equal HMAC_HEX, msg.envelope.hmac, "hmac generated and matches static data"

    thr.join
    rsock.close
    ctx.terminate
  end

  def test_base
    assert_raises ArgumentError do
      Hastur::Message::Base.new()
    end
    assert_raises ArgumentError do
      Hastur::Message::Base.new(1)
    end
    assert_raises ArgumentError do
      Hastur::Message::Base.new(1, 2)
    end
  end

  def test_stat
    e = Hastur::Envelope.new :route => :stat, :from => SecureRandom.uuid
    hmsg = Hastur::Message::Stat.new :envelope => e, :data => STAT
    refute_nil hmsg
    assert_kind_of Hastur::Message::Base, hmsg
    refute_nil hmsg.to_s
    refute_nil hmsg.payload

    assert_equal STAT_JSON, hmsg.payload
  end

  def test_route_id
    refute_nil Hastur.route_id(:stat)
    refute_nil Hastur.route_id('73746174-0000-0000-0000-000000000000')
    assert_equal '73746174-0000-0000-0000-000000000000', Hastur.route_id(:stat)
    assert_equal '73746174-0000-0000-0000-000000000000', Hastur.route_id('73746174-0000-0000-0000-000000000000')
  end

  def test_route_symbol
    refute_nil Hastur.route_symbol(:stat)
    refute_nil Hastur.route_symbol('73746174-0000-0000-0000-000000000000')
    assert_equal :stat, Hastur.route_symbol(:stat)
    assert_equal :stat, Hastur.route_symbol('73746174-0000-0000-0000-000000000000')
  end

  def test_error
    err = Hastur::Message::Error.new :payload => "eek!", :from => SecureRandom.uuid
    assert_kind_of Hastur::Message::Base, err
    assert_kind_of Hastur::Message::Error, err
  end

  # below are dumb placeholders for the moment
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

