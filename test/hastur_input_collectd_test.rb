#!/usr/bin/env ruby

require 'rubygems'
require 'minitest/autorun'

$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'hastur/input/collectd'

PACKET_DIR = File.join(File.dirname(__FILE__), 'data', 'collectd-raw-packets')

class TestHasturInputCollectd < MiniTest::Unit::TestCase

  # ruby hastur_collectd_test.rb --name test_custom_full_packet
  def test_custom_full_packet
    values = [
      4,                                                                 # n
      Hastur::Input::Collectd::DS_TYPE_COUNTER,  9876543210,             # CQ>
      Hastur::Input::Collectd::DS_TYPE_GAUGE,    3.14159265358979323846, # CE
      Hastur::Input::Collectd::DS_TYPE_DERIVE,   -111111111,             # Cq>
      Hastur::Input::Collectd::DS_TYPE_ABSOLUTE, 1234567890,             # CQ>
    ]

    values_p = values.pack(
      %w[n CQ> CE Cq> CQ>].join
    )

    vals = [ values_p.length, values_p ].pack("na*")

    data = [
      Hastur::Input::Collectd::TYPE_TIME,            12, 123,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_HOST,            11, "abcdef",        # nnZ6 (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_TIME_HR,         12, 321,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_PLUGIN,          11, "foobar",        # nnZ6  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_PLUGIN_INSTANCE, 8,  "b z",           # nnZ3  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_TYPE,            6,  "1",             # nnZ1  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_TYPE_INSTANCE,   12, "abcdefg",       # nnZ7  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_INTERVAL,        12, 456,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_INTERVAL_HR,     12, 654,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_MESSAGE,         10, "help!",         # nnZ5  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_SEVERITY,        12, 789,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_VALUES,          vals.length, vals    # nna*
    ]

    packet = data.pack(
      %w[nnQ nnZ6 nnQ nnZ6 nnZ3 nnZ1 nnZ7 nnQ nnQ nnZ5 nnQ nna*].join
    )

    stat = Hastur::Input::Collectd.decode_packet(packet, false)
    refute_nil stat
    assert_equal data[2],  stat[:host],           "check host value"
    assert_equal data[5],  stat[:time],           "check time value"
    assert_equal data[8],  stat[:time_hr],        "check hires time value"
    assert_equal data[11], stat[:plugin],         "check plugin value"
    assert_equal data[14], stat[:plugin_instance],"check plugin instance value"
    assert_equal data[17], stat[:type],           "check type value"
    assert_equal data[20], stat[:type_instance],  "check type instance value"
    assert_equal data[23], stat[:interval],       "check interval value"
    assert_equal data[26], stat[:interval_hr],    "check hires interval value"
    assert_equal data[29], stat[:message],        "check message value"
    assert_equal data[32], stat[:severity],       "check severity value"
  end

  def test_recorded_packets
    Dir.foreach(PACKET_DIR) do |file|
      path = File.join(PACKET_DIR, file)
      next unless File.file? path
      next unless file =~ /\d+\.bin$/
      msg = File.read(path)
      stat = Hastur::Input::Collectd.decode_packet(msg, false)
      refute_nil stat
      puts stat.inspect
    end
  end

  def test_binary_garbage
    100.times do
      msg = open("/dev/urandom", "rb") do |io|
        io.read(1000)
      end
      stat = Hastur::Input::Collectd.decode_packet(msg, true)
      assert_nil stat
    end
  end
end
