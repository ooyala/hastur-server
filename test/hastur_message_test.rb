#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'hastur/stat'
require 'hastur/message'

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

  def test_envelope
    uuid = SecureRandom.uuid
    e = Hastur::Envelope.new(
      :route     => :rawdata,
      :from      => uuid,
      :timestamp => 1328301436.9485276,
      :uptime    => 12.401439189910889,
      :sequence  => 1234
    )
    assert_equal false, e.ack? # should default to false
    assert_equal '72617764-6174-6100-0000-000000000000', e.to

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
    assert_equal 118,   noack.to_s.length
    assert_equal 59,    noack.pack.bytesize
    assert_equal '73746174-0000-0000-0000-000000000000', noack.to
  end

  def test_serialize
    zmq_part = "00670243f680d448deae3f2ca4513bb1e8"
    e = Hastur::Message::Rawdata.new(
      :from      => "be7f4980-6a1f-4120-b0ce-26de709afcf6",
      :payload   => "a b c d e f g",
      :timestamp => 1328301436.0000000,
      :uptime    => 12.400000000000000,
      :sequence  => 1234
    )

    json1 = "{\"klass\":\"Hastur::Message::Rawdata\",\"envelope\":{\"version\":1,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"be7f4980-6a1f-4120-b0ce-26de709afcf6\",\"ack\":0,\"sequence\":1234,\"timestamp\":1328301436.0,\"uptime\":12.4},\"payload\":\"a b c d e f g\",\"zmq_parts\":[]}"
    json2 = "{\"klass\":\"Hastur::Message::Rawdata\",\"envelope\":{\"version\":1,\"to\":\"72617764-6174-6100-0000-000000000000\",\"from\":\"be7f4980-6a1f-4120-b0ce-26de709afcf6\",\"ack\":0,\"sequence\":1234,\"timestamp\":1328301436.0,\"uptime\":12.4},\"payload\":\"a b c d e f g\",\"zmq_parts\":[\"#{zmq_part}\"]}"

    assert_equal json1, e.to_json

    e1 = Hastur::Message::Rawdata.from_json(json1)
    refute_nil e1
    assert e1.zmq_parts.empty?, "First json string has no zmq parts."

    e2 = Hastur::Message::Rawdata.from_json(json2)
    refute_nil e2
    refute e2.zmq_parts.empty?, "Second json string has a zmq part."
    assert_kind_of ZMQ::Message, e2.zmq_parts[0], "ZMQ part decoded correctly."

    assert_equal json1, e1.to_json, "re-encode decoded message should be exact same json"
    assert_equal json2, e2.to_json, "re-encode decoded message should be exact same json"
  end

  def test_base
    assert_raises ArgumentError do
      Hastur::Message::Base.new()
      Hastur::Message::Base.new(1)
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

  # below are dumb placeholders for the moment

  def test_error
    err = Hastur::Message::Error.new :payload => "eek!", :from => SecureRandom.uuid
    assert_kind_of Hastur::Message::Base, err
    assert_kind_of Hastur::Message::Error, err
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

