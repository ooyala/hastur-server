require_relative "../test_helper"
require "hastur-server/api/helpers"
require "hastur-server/time_util"
require "multi_json"

def make_messages(ts, count=1, values=nil, labels={})
  if values.nil?
    values = (0..count).map { |i| i }
  end
  series = {}
  0.upto(count-1) do |i|
    series[ts + i] = { "value" => values[i], "labels" => labels }
  end
  { "" => { "" => series } }
end

class HasturAPIHelpersTest < Scope::TestCase
  include Hastur::API::Helpers

  T1 = 1344293509 * 1_000_000
  T2 = T1 + Hastur::TimeUtil::USEC_ONE_DAY

  attr_reader :params, :env
  setup do
    @reftime = Hastur::TimeUtil.usec_epoch
    @params = {}
    @env = { :hastur_timestamp => @reftime }
    @fake_ts_counter = 0
  end

  context "get_start_end parameter parsing" do
    should "return exact values for start=<num>&end=<num>" do
      @params = { :start => T1, :end => T2 }
      start_ts, end_ts = get_start_end
      assert_equal T1, start_ts
      assert_equal T2, end_ts
    end

    should "return relative values for ago=<timespec>" do
      tspec = { "1000000" => 1, "one_minute" => 60, "five_minutes" => 300,
                "one_day" => 86400, "two_days" => 172800 }
      tspec.each do |ago, seconds|
        @params = { :ago => ago }
        start_ts, end_ts = get_start_end :one_day
        start_near = @reftime - seconds * 1_000_000

        # allow almost one second of splay
        assert (start_ts - start_near).abs < 999_999, "start_ts should be within 1s of current time - #{ago} seconds"
        assert (end_ts - @reftime).abs < 999_999, "end_ts should be within 1s of current time"
      end
    end
  end

  context "filtering by labels" do
    should "filter labels on a series of one entry" do
      s1 = make_messages 0, 1, [1], { "pid" => 1234 }
      out = filter_by_label(s1, ["pid:1234"])
      assert out[""][""].has_key? 0
      out = filter_by_label(s1, ["pid:1235"])
      refute out[""][""].has_key? 0
      out = filter_by_label(s1, ["!pid:1234"])
      refute out[""][""].has_key? 0
    end

    should "filter labels on a 100 element series" do
      s2 = make_messages 1, 100, nil, { "foo" => "bar", "baz" => "1" }

      out = filter_by_label(s2, ["!pid"])
      assert_equal 100, out[""][""].values.count

      out = filter_by_label(s2, ["foo"])
      assert_equal 100, out[""][""].values.count

      out = filter_by_label(s2, ["!foo"])
      assert_equal 0, out[""][""].values.count
    end
  end
end
