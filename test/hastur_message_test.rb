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
    assert_equal true,  acked.ack?
    assert_equal :stat, acked.route

    noack = Hastur::Envelope.new :route => :stat, :from => SecureRandom.uuid, :ack => false
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
    e = Hastur::Envelope.new :route => :stat, :from => SecureRandom.uuid
    hmsg = Hastur::Message::Stat.new :envelope => e, :data => STAT
    refute_nil hmsg
    assert_kind_of Hastur::Message::Base, hmsg
    refute_nil hmsg.to_s
    refute_nil hmsg.payload

    assert_equal STAT_JSON, hmsg.payload
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

