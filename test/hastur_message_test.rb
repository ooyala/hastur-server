#!/usr/bin/env ruby

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
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
    assert_raises ArgumentError do
      Hastur::Message::Envelope.new
      Hastur::Message::Envelope.new :foobar
      Hastur::Message::Envelope.new :stats # should be just "stat", this must error
      Hastur::Message::Envelope.new :sta   # should be just "stat", this must error
    end

    noopt = Hastur::Message::Envelope.new :stat
    assert_equal false, noopt.ack?
    assert_equal :stat, noopt.route
    assert_equal "v1\nstat\nack:0", noopt.to_s

    acked = Hastur::Message::Envelope.new :stat, true
    assert_equal true,  acked.ack?
    assert_equal :stat, acked.route
    assert_equal "v1\nstat\nack:1", acked.to_s

    noack = Hastur::Message::Envelope.new :stat, false
    assert_equal false, noack.ack?
    assert_equal :stat, noack.route
    assert_equal "v1\nstat\nack:0", noack.to_s
  end

  def test_base
    assert_raises ArgumentError do
      Hastur::Message::Base.new()
      Hastur::Message::Base.new(1)
      Hastur::Message::Base.new(1, 2)
    end
  end

  def test_stat
    hmsg = Hastur::Message::Stat.new(:stat => STAT_HASH)
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
    e = Hastur::Message::Error.new :error => "eek!"
    assert_kind_of Hastur::Message::Base, e
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

