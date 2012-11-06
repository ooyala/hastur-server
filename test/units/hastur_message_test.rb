#!/usr/bin/env ruby
require_relative "../test_helper"

require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'hastur-server/message'
require "mocha"

class TestClassHasturMessage < MiniTest::Unit::TestCase
  FROM_UUID = "01234567-89ab-cdef-deaf-cafedeadbeef"
  NONE_UUID = "00000000-0000-0000-0000-000000000000"

  # this should be consistent .... if another json encoder changes the order it will break
  STAT = {
    :type      => "counter",
    :name      => "foo.bar",
    :value     => 1024,
    :timestamp => 1329865874428623,
    :labels    => { :blahblah => 456, :units => "s" }
  }
  STAT_JSON = '{"type":"counter","name":"foo.bar","value":1024,"timestamp":1329865874428623,"labels":{"blahblah":456,"units":"s"}}'

  ENVELOPE = {
    :to        => NONE_UUID,
    :from      => "be7f4980-6a1f-4120-b0ce-26de709afcf6",
    :type      => :log,
    :timestamp => 1329865874428623,
    :uptime    => 12.400000000000000,
    :sequence  => 88888
  }

  SECRET = "123456"
  HMAC_HEX = "8861a1a7e8e826df5bee09da489f1c129c9b8b9c3ad96fee75f9ee1cc096fe99"
  INPROC_URI = "inproc://test1"

  def test_serialize
    zmq_part = "00670243f680d448deae3f2ca4513bb1e8"
    e = Hastur::Message::Log.new(ENVELOPE.merge(:payload => "a b c d e f g"))

    json1= "{\"type\":\"log\",\"envelope\":{\"version\":1,\"type_id\":2,\"to\":\"#{NONE_UUID}\",\"from\":\"be7f4980-6a1f-4120-b0ce-26de709afcf6\",\"ack\":0,\"resend\":0,\"sequence\":88888,\"timestamp\":1329865874428623,\"uptime\":12.4,\"hmac\":\"\",\"routers\":[]},\"payload\":\"a b c d e f g\",\"zmq_parts\":[]}"
    json2 = "{\"type\":\"log\",\"envelope\":{\"version\":1,\"type_id\":3,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"ac8084af-955f-4a48-ac8d-6d2c73f33a75\",\"ack\":1,\"resend\":0,\"sequence\":1234,\"timestamp\":1328301436000000,\"uptime\":15.9,\"hmac\":\"\",\"routers\":[]},\"payload\":\" 90ea9aa814354df5a4e82921c63a42cb \",\"zmq_parts\":[\"#{zmq_part}\"]}"

    assert_equal json1, e.to_json, "converting envelope to json matches static data"

    e1 = Hastur::Message::Log.from_json(json1)
    refute_nil e1
    assert e1.zmq_parts.empty?, "First json string has no zmq parts."

    e2 = Hastur::Message::Log.from_json(json2)
    refute_nil e2
    refute e2.zmq_parts.empty?, "Second json string has a zmq part."
    assert_kind_of ZMQ::Message, e2.zmq_parts[0], "ZMQ part decoded correctly."

    assert_equal json1, e1.to_json, "re-encode decoded message should be exact same json"
    assert_equal json2, e2.to_json, "re-encode decoded message should be exact same json"
  end

  def test_from_hash
    m1 = Hastur::Message.from_hash({
      :type => "event",
      :to => "8e58af00-708c-012f-e468-64ce8f3a9dc2",
      "from" => "fad92a20-7095-012f-e469-64ce8f3a9dc2",
      :ack => 1,
      :payload => '{ "type": "event", "name": "fake" }',
      :zmq_parts => [ "one", "two", "three" ]
    })

    assert_equal [ "one", "two", "three" ], m1.zmq_parts
    assert_equal Hastur::Message.symbol_to_type_id(:event), m1.type_id
    assert_equal "fad92a20-7095-012f-e469-64ce8f3a9dc2", m1.envelope.from
    assert_equal "8e58af00-708c-012f-e468-64ce8f3a9dc2", m1.envelope.to
    assert_equal true, m1.envelope.ack?
    assert_equal '{ "type": "event", "name": "fake" }', m1.payload
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
      m = Hastur::Message::Log.new(ENVELOPE.merge(:payload => "a b c d e f g"))

      m.transmit(@ssock, :secret => SECRET)
      @ssock.close
    end

    msg = Hastur::Message.recv(@rsock)
    assert_kind_of Hastur::Message::Log, msg
    assert_equal HMAC_HEX, msg.envelope.hmac, "hmac generated and matches static data"

    @rsock.close
    thr.join
    @ctx.terminate
  end

  def test_zmq_generic_error
    ctx = ZMQ::Context.new
    # Must use a real ZMQ::Socket or Message.recv fails
    mock_sock = ctx.socket(ZMQ::PAIR)

    # Test that with generic error we return -1
    ZMQ.expects(:errno).returns(ZMQ::EFAULT)
    mock_sock.expects(:recvmsgs).with([], 0).returns(-1)
    assert_raises(Hastur::ZMQError) do
      Hastur::Message.recv(mock_sock)
    end

    mock_sock.close
    ctx.terminate
  end

  def test_zmq_success
    ctx = ZMQ::Context.new
    # Must use a real ZMQ::Socket or Message.recv fails
    mock_sock = ctx.socket(ZMQ::PAIR)

    # Test with simple success in test-only mode
    mock_sock.expects(:recvmsgs).with([], 0).returns(0)
    assert_equal true, Hastur::Message.recv(mock_sock, 0, true)

    mock_sock.close
    ctx.terminate
  end

  def test_zmq_EAGAIN
    ctx = ZMQ::Context.new
    # Must use a real ZMQ::Socket or Message.recv fails
    mock_sock = ctx.socket(ZMQ::PAIR)

    # Test that with EAGAIN we return -1 with NonBlock flag
    ZMQ.expects(:errno).returns(ZMQ::EAGAIN)
    mock_sock.expects(:recvmsgs).with([], ZMQ::NonBlocking).returns(-1)
    assert_equal -1, Hastur::Message.recv(mock_sock, ZMQ::NonBlocking)

    mock_sock.close
    ctx.terminate
  end

  def test_zmq_EINTR_max_tries
    ctx = ZMQ::Context.new
    # Must use a real ZMQ::Socket or Message.recv fails
    mock_sock = ctx.socket(ZMQ::PAIR)

    # Test that if we return EINTR MAX_TRIES times, we error out
    ZMQ.expects(:errno).returns(ZMQ::EINTR).at_least_once
    mock_sock.expects(:recvmsgs).with([], 0).returns(-1).times(Hastur::Message::MAX_TRIES)
    assert_equal -1, Hastur::Message.recv(mock_sock)

    mock_sock.close
    ctx.terminate
  end

  def test_zmq_EINTR_once
    ctx = ZMQ::Context.new
    # Must use a real ZMQ::Socket or Message.recv fails
    mock_sock = ctx.socket(ZMQ::PAIR)

    # Test that if we return EINTR once then no error, everything works
    ZMQ.expects(:errno).returns(ZMQ::EINTR)
    mock_sock.expects(:recvmsgs).twice.with([], 0).returns(-1).then.returns(0)
    assert_equal true, Hastur::Message.recv(mock_sock, 0, true)

    mock_sock.close
    ctx.terminate
  end

  def test_log
    assert_raises ArgumentError do
      Hastur::Message::Log.new()
    end
    assert_raises ArgumentError do
      Hastur::Message::Log.new(1)
    end
    assert_raises ArgumentError do
      Hastur::Message::Log.new(1, 2)
    end
  end

  def test_stat
    e = Hastur::Envelope.new :type => :counter, :to => NONE_UUID, :from => SecureRandom.uuid
    hmsg = Hastur::Message::Stat::Counter.new :envelope => e, :data => STAT
    refute_nil hmsg
    assert_kind_of Hastur::Message::Base, hmsg
    refute_nil hmsg.to_s
    refute_nil hmsg.payload

    assert_equal STAT_JSON, hmsg.payload
  end

  def test_const_methods
    assert Hastur::Message.symbol?(:counter)
    assert Hastur::Message.symbol?(:gauge)
    assert Hastur::Message.symbol?(:mark)
    assert Hastur::Message.symbol?(:log)
    assert Hastur::Message.symbol?(:error)
    refute Hastur::Message.symbol?(:foobar)
    refute Hastur::Message.type_id?(0),  "0 must not be a message type"
    assert Hastur::Message.type_id?(4),  "2 must be a message type"
    assert Hastur::Message.type_id?(10), "10 must be a message type"
    assert Hastur::Message.type_id?(20), "20 must be a message type"
    assert Hastur::Message.type_id?(30), "30 must be a message type"
    refute Hastur::Message.type_id?(75), "75 must not be a message type"

    assert_equal Hastur::Message::Stat::Counter, Hastur::Message.symbol_to_class(:counter)
    assert_equal 12, Hastur::Message.symbol_to_type_id(:counter)
    assert_equal Hastur::Message::Stat::Gauge, Hastur::Message.type_id_to_class(11)
    assert_equal :mark, Hastur::Message.type_id_to_symbol(10)
    refute_equal Hastur::Message::Stat::Gauge, Hastur::Message.symbol_to_class(:error)
    refute_equal Hastur::Message::Stat::Counter, Hastur::Message.type_id_to_class(11)
  end

  def test_error
    err = Hastur::Message::Error.new :payload => "eek!", :from => SecureRandom.uuid
    assert_kind_of Hastur::Message::Base, err
    assert_kind_of Hastur::Message::Error, err
  end
end
