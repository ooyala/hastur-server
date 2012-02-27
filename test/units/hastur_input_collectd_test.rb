#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', '..', 'lib')

require 'rubygems'
require 'minitest/autorun'

require 'hastur-server/input/collectd'

PACKET_DIR = File.join(File.dirname(__FILE__), '..', 'data', 'collectd-raw-packets')

class TestHasturInputCollectd < MiniTest::Unit::TestCase

  # ruby hastur_collectd_test.rb --name test_custom_full_packet
  def test_custom_full_packet
    values = [
      4,                                         # n
      Hastur::Input::Collectd::DS_TYPE_COUNTER,  # C
      Hastur::Input::Collectd::DS_TYPE_GAUGE,    # C
      Hastur::Input::Collectd::DS_TYPE_DERIVE,   # C
      Hastur::Input::Collectd::DS_TYPE_ABSOLUTE, # C
      9876543210,                                # CQ>
      3.14159265358979323846,                    # CE
      -111111111,                                # Cq>
      1234567890,                                # CQ>
    ]

    vals = values.pack( %w[n C4 Q> E q> Q>].join )

    data = [
      Hastur::Input::Collectd::TYPE_HOST,            11, "abcdef",        # nnZ7 (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_TIME,            12, 123,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_TIME_HR,         12, 321,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_PLUGIN,          11, "foobar",        # nnZ7  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_PLUGIN_INSTANCE, 8,  "b z",           # nnZ4  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_TYPE,            6,  "1",             # nnZ2  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_TYPE_INSTANCE,   12, "abcdefg",       # nnZ8  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_INTERVAL,        12, 456,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_INTERVAL_HR,     12, 654,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_MESSAGE,         10, "help!",         # nnZ6  (4 + strlen + 1)
      Hastur::Input::Collectd::TYPE_SEVERITY,        12, 789,             # nnQ uint64_t
      Hastur::Input::Collectd::TYPE_VALUES,       vals.bytesize + 4, vals # nna*
    ]

    packet = data.pack(
      %w[nnZ7 nnQ nnQ nnZ7 nnZ4 nnZ2 nnZ8 nnQ nnQ nnZ6 nnQ nna*].join
    )

    stat = Hastur::Input::Collectd.decode_packet(packet)
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
      stat = Hastur::Input::Collectd.decode_packet(msg)
      refute_nil stat

      # the packets this was written against were recorded on one box, hostname hardcoded here
      # the collectd configuration had "Hostname spaceghost.ooyala.com" 
      assert_equal "spaceghost.ooyala.com", stat[:host], "check hostname"
      
      # TODO: MOAR!
    end
  end

  def test_binary_garbage
    100.times do
      msg = open("/dev/urandom", "rb") do |io|
        io.read(1000)
      end
      assert_raises Hastur::PacketDecodingError do
        stat = Hastur::Input::Collectd.decode_packet(msg)
      end

      stat = Hastur::Input::Collectd.decode(msg)
      assert_nil stat
    end
  end

  # Collectd uses NaN (or causes NaN) for incomplete values, like pings with no response.
  # this breaks json encoders, so make sure we can encode whatever comes out. 
  # This is a packet captured via cut/paste from a real failure.
  def test_nan_content
    packet = "\x00\x00\x00\x1Aspaceghost.ooyala.com\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x038\xAF\x1C\x00\t\x00\f\x00\x00\x00\x02\x80\x00\x00\x00\x00\x02\x00\adf\x00\x00\x03\x00\troot\x00\x00\x04\x00\x0Fdf_complex\x00\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\xEBz\xFAA\x00\b\x00\f\x13\xD0\xB8\xDA\x039\e\x04\x00\x04\x00\x0Edf_inodes\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x039C\x85\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x039g\x96\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x039\x8D\x1D\x00\x03\x00\thome\x00\x00\x04\x00\x0Fdf_complex\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\xA0@\xE6-B\x00\b\x00\f\x13\xD0\xB8\xDA\x039\xAAv\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x058\xF2A\x00\b\x00\f\x13\xD0\xB8\xDA\x039\xC0\xBC\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\xF3\x99\x1AB\x00\b\x00\f\x13\xD0\xB8\xDA\x039\xD6\xEE\x00\x04\x00\x0Edf_inodes\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00@\xD5\x10UA\x00\b\x00\f\x13\xD0\xB8\xDA\x039\xEEf\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\x01\xC9\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\xACr\eA\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\x1Fm\x00\x03\x00\rhome-old\x00\x00\x04\x00\x0Fdf_complex\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x80\xCC\xF0\nB\x00\b\x00\f\x13\xD0\xB8\xDA\x03:9E\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\xE0A\x00\b\x00\f\x13\xD0\xB8\xDA\x03:P\xF8\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00*r\x18B\x00\b\x00\f\x13\xD0\xB8\xDA\x03:i\x8D\x00\x04\x00\x0Edf_inodes\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x95\xAA@A\x00\b\x00\f\x13\xD0\xB8\xDA\x03:~\xD6\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\x93\xA2\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00X\xAB\x1AA\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\xB1\xB3\x00\x03\x00\fmnt-tmp\x00\x00\x04\x00\x0Fdf_complex\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00n\xD3\xE7A\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\xCAe\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\xD0A\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\xE2\"\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x80\xAA\xEB\x0FB\x00\b\x00\f\x13\xD0\xB8\xDA\x03:\xF7\xA8\x00\x04\x00\x0Edf_inodes\x00\x00\x05\x00\tfree\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\xA2\x13*A\x00\b\x00\f\x13\xD0\xB8\xDA\x03;\r\xE1\x00\x05\x00\rreserved\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\b\x00\f\x13\xD0\xB8\xDA\x03; g\x00\x05\x00\tused\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\xBC\xD8\eA\x00\b\x00\f\x13\xD0\xB8\xDA\xA7p\e}\x00\x02\x00\tping\x00\x00\x03\x00\x05\x00\x00\x04\x00\tping\x00\x00\x05\x00\x10al-dev1.sv2\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\xF8\x7F\x00\b\x00\f\x13\xD0\xB8\xDA\xA7p\x94+\x00\x04\x00\x10ping_stddev\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\xF8\x7F\x00\b\x00\f\x13\xD0\xB8\xDA\xA7p\x95\xB6\x00\x02\x00\bcpu\x00\x00\x03\x00\x060\x00\x00\x04\x00\bcpu\x00\x00\x05\x00\tuser\x00\x00\x06\x00\x0F\x00\x01\x02\x00\x00\x00\x00\x00\xC2_\x85\x00\b\x00\f\x13\xD0\xB8\xDA\xA7p\xAE\xAF\x00\x02\x00\tping\x00\x00\x03\x00\x05\x00\x00\x04\x00\x12ping_droprate\x00\x00\x05\x00\x10al-dev1.sv2\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\xF0?\x00\b\x00\f\x13\xD0\xB8\xDA\xA7p\xD4K\x00\x04\x00\tping\x00\x00\x05\x00\x13www.tobert.org\x00\x00\x06\x00\x0F\x00\x01\x01\x00\x00\x00\x00\x00\x00\xF8\x7F"

      stat = Hastur::Input::Collectd.decode(packet)
      refute_nil stat, "Should be able to decode packets containing NaN"
      json = MultiJson.encode(stat)
      refute_nil json, "Should be able to JSON encode packets containing NaN"
      data = MultiJson.decode(json)
      refute_nil data, "Should be able to decode JSON that was once data continaing NaN"
  end
end
