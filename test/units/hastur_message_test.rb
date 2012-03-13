#!/usr/bin/env ruby
require_relative "../test_helper"

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
    :type      => :rawdata,
    :timestamp => 1329865874428623,
    :uptime    => 12.400000000000000,
    :sequence  => 88888
  }

  SECRET = "123456"
  HMAC_HEX = "8861a1a7e8e826df5bee09da489f1c129c9b8b9c3ad96fee75f9ee1cc096fe99"
  INPROC_URI = "inproc://test1"

  def test_serialize
    zmq_part = "00670243f680d448deae3f2ca4513bb1e8"
    e = Hastur::Message::Rawdata.new(ENVELOPE.merge(:payload => "a b c d e f g"))

    json1= "{\"klass\":\"Hastur::Message::Rawdata\",\"envelope\":{\"version\":1,\"type\":6,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"be7f4980-6a1f-4120-b0ce-26de709afcf6\",\"ack\":0,\"resend\":0,\"sequence\":88888,\"timestamp\":1329865874428623,\"uptime\":12.4,\"hmac\":\"\",\"routers\":[]},\"payload\":\"a b c d e f g\",\"zmq_parts\":[]}"
    json2 = "{\"klass\":\"Hastur::Message::Log\",\"envelope\":{\"version\":1,\"type\":3,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"ac8084af-955f-4a48-ac8d-6d2c73f33a75\",\"ack\":1,\"resend\":0,\"sequence\":1234,\"timestamp\":1328301436000000,\"uptime\":15.9,\"hmac\":\"\",\"routers\":[]},\"payload\":\" 90ea9aa814354df5a4e82921c63a42cb \",\"zmq_parts\":[\"#{zmq_part}\"]}"

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
    @ctx = ZMQ::Context.new
    @rsock = @ctx.socket(ZMQ::PAIR)
    @ssock = @ctx.socket(ZMQ::PAIR)
    @rsock.bind(INPROC_URI)
    @ssock.connect(INPROC_URI)

    thr = Thread.new do
      Thread.current.abort_on_exception=true
      e = Hastur::Envelope.new(ENVELOPE)
      m = Hastur::Message::Rawdata.new(ENVELOPE.merge(:payload => "a b c d e f g"))

      m.send(@ssock, :secret => SECRET)
      @ssock.close
    end

    msg = Hastur::Message.recv(@rsock)
    assert_kind_of Hastur::Message::Rawdata, msg
    assert_equal HMAC_HEX, msg.envelope.hmac, "hmac generated and matches static data"

    @rsock.close
    thr.join
    @ctx.terminate
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
    e = Hastur::Envelope.new :type => :stat, :from => SecureRandom.uuid
    hmsg = Hastur::Message::Stat.new :envelope => e, :data => STAT
    refute_nil hmsg
    assert_kind_of Hastur::Message::Base, hmsg
    refute_nil hmsg.to_s
    refute_nil hmsg.payload

    assert_equal STAT_JSON, hmsg.payload
  end

  def test_const_methods
    stat_uuid = '73746174-0000-0000-0000-000000000000'
    log_uuid  = '6c6f6700-0000-0000-0000-000000000000'
    assert Hastur::Message.symbol?(:stat)
    assert Hastur::Message.symbol?(:log)
    assert Hastur::Message.symbol?(:error)
    assert Hastur::Message.symbol?(:registration)
    assert Hastur::Message.route_uuid?(stat_uuid)
    assert Hastur::Message.route_uuid?(log_uuid)
    assert Hastur::Message.type_id?(1)
    assert Hastur::Message.type_id?(9)

    assert_equal Hastur::Message::Stat, Hastur::Message.symbol_to_class(:stat)
    assert_equal Hastur::Message::Stat, Hastur::Message.route_uuid_to_class(stat_uuid)
    assert_equal Hastur::Message::Stat, Hastur::Message.type_id_to_class(1)
    refute_equal Hastur::Message::Stat, Hastur::Message.symbol_to_class(:error)
    refute_equal Hastur::Message::Stat, Hastur::Message.route_uuid_to_class(log_uuid)
    refute_equal Hastur::Message::Stat, Hastur::Message.type_id_to_class(9)
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
  def test_registration
    Hastur::Message::Registration
  end
end

