require_relative "../test_helper"
require "hastur-server/aggregation"

UUID1 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
UUID2 = "11111111-2222-3333-4444-555555555555"
UUID1_BAZ_SUM = 21
UUID1_WIN_SUM = 441
UUID2_WIN_SUM = 462
SERIES = {
  UUID1 => {
    "foo.bar.baz" => {
      1341965000000000 => 1, 1341965100000000 => 1, 1341965200000000 => 1,
      1341965300000000 => 1, 1341965400000000 => 1, 1341965500000000 => 1,
      1341965600000000 => 1, 1341965700000000 => 1, 1341965800000000 => 1,
      1341965900000000 => 1, 1341966000000000 => 1, 1341966100000000 => 1,
      1341966200000000 => 1, 1341966300000000 => 1, 1341966400000000 => 1,
      1341966500000000 => 1, 1341966600000000 => 1, 1341966700000000 => 1,
      1341966800000000 => 1, 1341966900000000 => 1, 1341967100000000 => 1
    }.freeze,
    "foo.bar.win" => {
      1341965000000000 => 1,  1341965100000000 => 3,  1341965200000000 => 5,
      1341965300000000 => 7,  1341965400000000 => 9,  1341965500000000 => 11,
      1341965600000000 => 13, 1341965700000000 => 15, 1341965800000000 => 17,
      1341965900000000 => 19, 1341966000000000 => 21, 1341966100000000 => 23,
      1341966200000000 => 25, 1341966300000000 => 27, 1341966400000000 => 29,
      1341966500000000 => 31, 1341966600000000 => 33, 1341966700000000 => 35,
      1341966800000000 => 37, 1341966900000000 => 39, 1341967100000000 => 41
    }.freeze
  }.freeze,
  UUID2 => {
    "foo.bar.win" => {
      1341965000000000 => 2,  1341965100000000 => 4,  1341965200000000 => 6,
      1341965300000000 => 8,  1341965400000000 => 10, 1341965500000000 => 12,
      1341965600000000 => 14, 1341965700000000 => 16, 1341965800000000 => 18,
      1341965900000000 => 20, 1341966000000000 => 22, 1341966100000000 => 24,
      1341966200000000 => 26, 1341966300000000 => 28, 1341966400000000 => 30,
      1341966500000000 => 32, 1341966600000000 => 34, 1341966700000000 => 36,
      1341966800000000 => 38, 1341966900000000 => 40, 1341967100000000 => 42
    }.freeze
  }.freeze
}.freeze

class AggregationTest < Scope::TestCase
  context "verify expression parsing works for basic aggregations" do
    should "last(sum())" do
      sum = Hastur::Aggregation.evaluate("last(sum())", SERIES)
      refute_nil sum
      assert_kind_of Hash, sum
      assert_kind_of Hash, sum[UUID1]
      assert_kind_of Hash, sum[UUID2]
      assert_kind_of Hash, sum[UUID1]["foo.bar.baz"]
      assert_kind_of Hash, sum[UUID1]["foo.bar.win"]
      assert_kind_of Hash, sum[UUID2]["foo.bar.win"]
      assert_equal UUID1_BAZ_SUM, sum[UUID1]["foo.bar.baz"].values.first
      assert_equal UUID1_WIN_SUM, sum[UUID1]["foo.bar.win"].values.first
      assert_equal UUID2_WIN_SUM, sum[UUID2]["foo.bar.win"].values.first
    end

    should "diff()" do
      diff = Hastur::Aggregation.evaluate("diff()", SERIES)
      assert_equal  0, diff[UUID1]["foo.bar.baz"][1341965000000000]
      assert_equal  0, diff[UUID1]["foo.bar.baz"][1341965100000000]
      assert_equal  0, diff[UUID1]["foo.bar.baz"][1341965200000000]
      assert_equal  0, diff[UUID1]["foo.bar.win"][1341965000000000]
      assert_equal -2, diff[UUID1]["foo.bar.win"][1341965100000000]
      assert_equal  0, diff[UUID2]["foo.bar.win"][1341965000000000]
      assert_equal -2, diff[UUID2]["foo.bar.win"][1341965100000000]
      assert_equal -2, diff[UUID2]["foo.bar.win"][1341967100000000]
    end

    should "merge(uuid,diff())" do
      series = Hastur::Aggregation.evaluate("merge(uuid,diff())", SERIES)
      assert series.has_key? ""
      refute series.has_key? UUID1
      refute series.has_key? UUID2
    end

    should "merge(name,diff())" do
      series = Hastur::Aggregation.evaluate("merge(name,diff())", SERIES)
      assert series[UUID1].has_key? ""
      assert series[UUID2].has_key? ""
      refute series[UUID1].has_key? "foo.bar.baz"
      refute series[UUID1].has_key? "foo.bar.win"
      refute series[UUID2].has_key? "foo.bar.win"
    end

    should "diff(merge(uuid))" do
      series = Hastur::Aggregation.evaluate("diff(merge(uuid))", SERIES)
      assert series.has_key? ""
    end

    should "sum(merge(name))" do
      series = Hastur::Aggregation.evaluate("sum(merge(name))", SERIES)
      assert series[UUID1].has_key? ""
      assert series[UUID2].has_key? ""
      assert_equal UUID1_BAZ_SUM, series[UUID1][""][1341967100000000]
      assert_equal UUID2_WIN_SUM, series[UUID2][""][1341967100000000]
    end

    should "max()" do
      series = Hastur::Aggregation.evaluate("max()", SERIES)
      assert_equal  1, series[UUID1]["foo.bar.baz"].values.first
      assert_equal 41, series[UUID1]["foo.bar.win"].values.first
      assert_equal 42, series[UUID2]["foo.bar.win"].values.first
    end

    should "min()" do
      series = Hastur::Aggregation.evaluate("min()", SERIES)
      assert_equal 1, series[UUID1]["foo.bar.baz"].values.first
      assert_equal 1, series[UUID1]["foo.bar.win"].values.first
      assert_equal 2, series[UUID2]["foo.bar.win"].values.first
    end
  end
end

