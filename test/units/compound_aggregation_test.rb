require_relative "../test_helper"
require "hastur-server/aggregation"

UUID1 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
UUID2 = "11111111-2222-3333-4444-555555555555"

SERIES1 = {
  UUID1 => {
    "linux.proc.uptime" => {
      1341965090094427 => {"uptime" => 1337247, "idle" => 10246224},
      1341982283412743 => {"uptime" => 1337257, "idle" => 10246297},
      1341982293435449 => {"uptime" => 1337267, "idle" => 10246368},
      1341982303454134 => {"uptime" => 1337277, "idle" => 10246441},
      1341982313485386 => {"uptime" => 1337287, "idle" => 10246512},
      1341982323504596 => {"uptime" => 1337297, "idle" => 10246585},
      1341982333522966 => {"uptime" => 1337307, "idle" => 10246655},
      1341982343542136 => {"uptime" => 1337317, "idle" => 10246728},
      1341982353559244 => {"uptime" => 1337327, "idle" => 10246800},
      1341982363578043 => {"uptime" => 1337337, "idle" => 10246874},
      1341982373595100 => {"uptime" => 1337347, "idle" => 10246945},
      1341982383613639 => {"uptime" => 1337357, "idle" => 10247018},
      1341982393631004 => {"uptime" => 1337367, "idle" => 10247089},
      1341982403649701 => {"uptime" => 1337377, "idle" => 10247163},
      1341982413666583 => {"uptime" => 1337387, "idle" => 10247234},
      1341982423685420 => {"uptime" => 1337397, "idle" => 10247307}
    }.freeze
  }.freeze
}.freeze

SERIES2 = {
  UUID2 => {
    "linux.proc.stat" => {
      1341984487814433 => {
        "cpu" => [29890338,2131184,11798959,1021907636,916064,308,263361,0,10219,0].freeze,
        "cpu0" => [6416206,314277,3054030,121797218,607166,308,216808,0,1868,0].freeze,
        "cpu1" => [7373788,380985,2287374,122860995,144901,0,21276,0,1825,0].freeze,
        "cpu2" => [6412824,389921,2777820,123472164,66262,0,7589,0,1878,0].freeze,
        "cpu3" => [6775931,409961,2140714,123767743,41887,0,12092,0,1955,0].freeze,
        "cpu4" => [667589,105723,256528,132757096,12760,0,1245,0,689,0].freeze,
        "cpu5" => [891743,186979,436544,132219005,13416,0,1057,0,611,0].freeze,
        "cpu6" => [685334,153535,469296,132477310,14294,0,1793,0,658,0].freeze,
        "cpu7" => [666919,189798,376649,132556101,15374,0,1496,0,729,0].freeze,
        "intr" => 2527592039,
        "ctxt" => 6226491234,
        "btime" => 1340645026,
        "processes" => 136317,
        "procs_running" => 2,
        "procs_blocked" => 0,
        "softirq" => 4137735242
      }.freeze,
      1341984497833023 => {
        "cpu" => [29890665,2131197,11799233,1021914919,916066,308,263364,0,10219,0].freeze,
        "cpu0" => [6416294,314282,3054076,121798056,607168,308,216810,0,1868,0].freeze,
        "cpu1" => [7373849,380987,2287431,122861856,144901,0,21277,0,1825,0].freeze,
        "cpu2" => [6412881,389923,2777885,123473026,66262,0,7590,0,1878,0].freeze,
        "cpu3" => [6776005,409964,2140755,123768602,41887,0,12092,0,1956,0].freeze,
        "cpu4" => [667599,105724,256557,132758046,12760,0,1245,0,689,0].freeze,
        "cpu5" => [891759,186980,436551,132219978,13416,0,1057,0,611,0].freeze,
        "cpu6" => [685344,153535,469305,132478285,14294,0,1793,0,658,0].freeze,
        "cpu7" => [666931,189798,376669,132557066,15374,0,1496,0,729,0].freeze,
        "intr" => 2527642323,
        "ctxt" => 6226611475,
        "btime" => 1340645026,
        "processes" => 136318,
        "procs_running" => 1,
        "procs_blocked" => 0,
        "softirq" => 4137781204
      }.freeze
    }.freeze
  }.freeze
}.freeze

class CompoundAggregationTest < Scope::TestCase
  context "verify expression parsing works for compound aggregations" do
    should "compound(uptime)" do
      series = Hastur::Aggregation.evaluate("compound(uptime)", SERIES1, {})
      puts series
      assert series.has_key? UUID1
    end
    should "compound(uptime,idle)" do
      series = Hastur::Aggregation.evaluate("compound(uptime,idle)", SERIES1, {})
      assert series.has_key? UUID1
    end
    should "compound_list(cpu)" do
      series = Hastur::Aggregation.evaluate("compound_list(cpu)", SERIES2, {})
      assert series.has_key? UUID2
    end
    should "compound_list(cpu0,cpu1,cpu2)" do
      series = Hastur::Aggregation.evaluate("compound_list(cpu0,cpu1,cpu2)", SERIES2, {})
      assert series.has_key? UUID2
    end
    should "compound(processes,procs_running,procs_blocked)" do
      series = Hastur::Aggregation.evaluate(
        "compound(processes,procs_running,procs_blocked)",
        SERIES2, {}
      )
      assert series.has_key? UUID2
    end
  end
end
