require_relative "../test_helper"
require "hastur-server/time_util"
require "date"

class TimeUtilTest < Scope::TestCase
  include Hastur::TimeUtil

  context "convert to seconds" do
    should "convert to truncated seconds" do
      assert_equal 1_234_567_890, usec_to_sec(1_234_567_890_000_000)
      assert_equal 1_111_111_111, usec_to_sec(1_111_111_111_111_111)
      assert_equal 1_000_000_000, usec_to_sec(1_000_000_000_000_009)
    end

    should "convert to ruby time" do
      assert_equal Time.at(1_234_567_890), usec_to_time(1_234_567_890_000_000)
      assert_equal Time.at(1_111_111_111), usec_to_time(1_111_111_111_111_111)
      assert_equal Time.at(1_000_000_000), usec_to_time(1_000_000_000_000_009)
    end
  end

  context "truncate times" do
    should "convert to ruby time" do
      assert_equal 1234483200000000, usec_truncate(1_234_567_898_765_432, :day)
    end
  end

  context "list aligned chunks" do
    should "return a list of one time when start/end are equal" do
      assert_equal [1234567800000000], usec_aligned_chunks(1234567898765432, 1234567898765432, :five_minutes)
      assert_equal [1234567860000000], usec_aligned_chunks(1234567898765432, 1234567898765432, :minute)
      assert_equal [1234483200000000], usec_aligned_chunks(1234567898765432, 1234567898765432, :day)

      assert_equal [1111110900000000], usec_aligned_chunks(1111111111111111, 1111111111111111, :five_minutes)
      assert_equal [1111111080000000], usec_aligned_chunks(1111111111111111, 1111111111111111, :minute)
      assert_equal [1111104000000000], usec_aligned_chunks(1111111111111111, 1111111111111111, :day)
    end

    should "return one chunk when start/end delta is < chunk and a boundary isn't crossed" do
      assert_equal [1234567800000000], usec_aligned_chunks(1234567898765432, 1234567899765432, :five_minutes)
      assert_equal [1234567860000000], usec_aligned_chunks(1234567898765432, 1234567899765432, :minute)
    end

    should "return two chunks when start/end delta is < chunk and a boundary is crossed" do
      assert_equal [1234567800000000, 1234568100000000], usec_aligned_chunks(1234567898765432, 1234568258765432, :five_minutes)
      assert_equal [1234567860000000, 1234567920000000], usec_aligned_chunks(1234567898765432, 1234567958765432, :minute)
    end

    should "return an array with 60 items in it" do
      assert_equal 60, usec_aligned_chunks(0, USEC_ONE_HOUR - 1,   :minute).length
      assert_equal 60, usec_aligned_chunks(0, USEC_ONE_MINUTE - 1, :second).length
    end

    should "return an array with 61 items in it (inclusive upper bound)" do
      assert_equal 61, usec_aligned_chunks(0, USEC_ONE_HOUR,   :minute).length
      assert_equal 61, usec_aligned_chunks(0, USEC_ONE_MINUTE, :second).length
    end

    should "follow the rules for five minute chunks" do
      # 5:00 to 5:00 --> 5:00
      # 5:01 to 5:04 --> 5:00
      # 5:01 to 5:06 --> 5:00, 5:05
      # 5:01 to 5:14 --> 5:00, 5:05, 5:10
      # 5:01 to 5:15 --> 5:00, 5:05, 5:10, 5:15
      # 5:01 to 5:16 --> 5:00, 5:05, 5:10, 5:15
      times = {}
      59.times do |x|
        times["5:#{sprintf '%02d', x}"] = usec_epoch(Time.utc(2012, 05, 18, 17, x))
      end

      assert_equal [times["5:00"]], usec_aligned_chunks(times["5:00"], times["5:00"], :five_minutes)
      assert_equal [times["5:00"]], usec_aligned_chunks(times["5:00"], times["5:01"], :five_minutes)

      assert_equal [times["5:00"], times["5:05"], times["5:10"], times["5:15"]],
        usec_aligned_chunks(times["5:00"], times["5:16"], :five_minutes)

      assert_equal 1, usec_aligned_chunks(times["5:00"], times["5:00"], :five_minutes).length
      assert_equal 1, usec_aligned_chunks(times["5:01"], times["5:04"], :five_minutes).length
      assert_equal 2, usec_aligned_chunks(times["5:01"], times["5:06"], :five_minutes).length
      assert_equal 3, usec_aligned_chunks(times["5:01"], times["5:14"], :five_minutes).length
      assert_equal 4, usec_aligned_chunks(times["5:01"], times["5:15"], :five_minutes).length
      assert_equal 4, usec_aligned_chunks(times["5:00"], times["5:16"], :five_minutes).length
    end
  end
end
