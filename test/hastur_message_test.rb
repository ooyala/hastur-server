#!/usr/bin/env ruby

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require_relative '../lib/hastur/stat'
require_relative '../lib/hastur/message'

class TestClassHasturMessage < MiniTest::Unit::TestCase
  # this should be consistent .... if another json encoder changes the order it will break
  STAT_JSON = '{"name":"foo.bar","value":1024,"units":"s","timestamp":1328176249.1028926,"tags":{"blahblah":456}}'
  STAT_HASH = {
    :name      => "foo.bar",
    :value     => 1024,
    :units     => "s",
    :timestamp => 1328176249.1028926,
    :tags      => { :blahblah => 456 }
  }
  STAT_OBJECT = Hastur::Stat.new(STAT_HASH)

  def setup
    @ctx = ZMQ::Context.new(0)
    @req = @ctx.socket(ZMQ::REQ)
    @rep = @ctx.socket(ZMQ::REP)
    @req.bind("inproc://test")
    @rep.connect("inproc://test")
  end

  def test_envelope
    uuid = SecureRandom.uuid
    noopt = Hastur::Envelope.new :route => :rawdata, :uuid => uuid
    assert_equal false, noopt.ack? # should default to false
    assert_equal :rawdata, noopt.route
    expect = "000172617764617461000000000000000000" + uuid.split(/-/).join + "0000" +
      "6170706c69636174696f6e2f6a736f6e00000000000000000000000000000000"
    assert_equal expect, noopt.to_s
    assert_equal [expect].pack('H*'), noopt.pack

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
    assert_equal 136,   noack.to_s.length
    assert_equal 68,    noack.pack.bytesize
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
    hmsg = Hastur::Message::Stat.new :envelope => e, :stat => STAT_HASH
    refute_nil hmsg
    assert_kind_of Hastur::Message::Base, hmsg
    refute_nil hmsg.to_s
    refute_nil hmsg.payload

    assert_equal STAT_JSON, hmsg.payload

    hmsg.send(@req)
    sent = Hastur::Message.recv(@rep)
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

