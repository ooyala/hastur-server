#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'hastur-server/envelope'
require 'hastur-server/message'

class TestClassHasturEnvelope < MiniTest::Unit::TestCase
  UUID = "01234567-89ab-cdef-deaf-cafedeadbeef"
  ENVELOPE = {
    :from      => "be7f4980-6a1f-4120-b0ce-26de709afcf6",
    :type      => :rawdata,
    :timestamp => 1329865874428623,
    :uptime    => 12.400000000000000,
    :sequence  => 88888
  }

  def test_envelope
    e = Hastur::Envelope.new(
      :from      => UUID,
      :type      => :rawdata,
      :timestamp => 1328301436948527,
      :uptime    => 12.401439189910889,
      :sequence  => 1234
    )
    assert_equal false, e.ack? # should default to false
    assert_equal '72617764-6174-6100-0000-000000000000', e.to

    ehex = e.to_s # returns envelope in hex
    assert_equal "0001",               ehex[0,  4 ], "check version"
    assert_equal "72617764617461",     ehex[6,  14], "check route"
    assert_equal UUID.split(/-/).join, ehex[38, 32], "check uuid"

    assert_raises ArgumentError, "empty args should raise an exception" do
      Hastur::Envelope.new
    end

    assert_raises TypeError, "invalid args should raise an exception" do
      Hastur::Envelope.new :foobar
    end

    # :to and :from are both required
    assert_raises ArgumentError, "missing :from should throw an exception" do
      Hastur::Envelope.new :from => SecureRandom.uuid
    end
    assert_raises ArgumentError, "missing :to should throw an exception" do
      Hastur::Envelope.new :to   => SecureRandom.uuid
    end

    acked = Hastur::Envelope.new(
      :type => :stat,
      :from => SecureRandom.uuid.split(/-/).join,
      :ack  => true
    )
    assert_equal true, acked.ack?
    assert_equal '73746174-0000-0000-0000-000000000000', acked.to

    noack = Hastur::Envelope.new(
      :type => :stat,
      :from => SecureRandom.uuid,
      :ack => false
    )
    assert_equal false, noack.ack?
    assert_equal 186,   noack.to_s.length
    assert_equal 93,    noack.pack.bytesize
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
end
