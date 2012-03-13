require_relative "../../test_helper"

require "hastur-server/sink/cassandra_rollups"

# Timestamps to test with
TESTING_NOW_TS = 1330718485000000

# Tues, March 6th
TUESDAY_TS = Time.utc(2012, 3, 6).to_i * 1_000_000
WEDNESDAY_TS = Time.utc(2012, 3, 7).to_i * 1_000_000
THURSDAY_TS = Time.utc(2012, 3, 8).to_i * 1_000_000
FRIDAY_TS = Time.utc(2012, 3, 9).to_i * 1_000_000
SATURDAY_TS = Time.utc(2012, 3, 10).to_i * 1_000_000

# Sun, March 11th
NEXT_SUNDAY_TS = Time.utc(2012, 3, 11).to_i * 1_000_000

# Sun, March 18th
NEXT_NEXT_SUNDAY_TS = Time.utc(2012, 3, 18).to_i * 1_000_000
# Mon, March 19th
NEXT_NEXT_MONDAY_TS = Time.utc(2012, 3, 20).to_i * 1_000_000
# Tues, March 20th
NEXT_NEXT_TUESDAY_TS = Time.utc(2012, 3, 20).to_i * 1_000_000

# These are granularities and so are in seconds, not usec.
# Hm...  Maybe need to fix that :-/
ONE_HOUR = 60 * 60
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY

class CassandraRollupsTest < Scope::TestCase
  setup do
    @cass_client = mock("Cassandra client")
    @cass_client.stubs(:batch).yields(@cass_client)
    Hastur::Util.stubs(:timestamp).with(nil).returns(TESTING_NOW_TS)
  end

  context "with rollup segments" do
    should "return last time segment for timestamp" do
      five_minute_time = 5*60*1_000_000
      last_time_segment = Hastur::Cassandra.last_time_segment_for_timestamp(
                                    five_minute_time + 1000,
                                    Hastur::Cassandra::FIVE_MINUTES)
      assert_equal(five_minute_time, last_time_segment)
    end

    should "return next time segment for timestamp" do
      five_minute_time = 5*60*1_000_000
      next_time_segment = Hastur::Cassandra.next_time_segment_for_timestamp(
                                    five_minute_time + 1000,
                                    Hastur::Cassandra::FIVE_MINUTES)
      assert_equal(five_minute_time + five_minute_time, next_time_segment)
    end
  end

=begin
  context "with rollup segments" do
    should "..." do
      segs = Hastur::Cassandra.get_granular_segments_from_timestamps(TUESDAY_TS,
                                        NEXT_NEXT_TUESDAY_TS, [ ONE_WEEK, ONE_DAY ])
      assert_equal [
                    [TUESDAY_TS, WEDNESDAY_TS - 1],
                    [WEDNESDAY_TS, THURSDAY_TS - 1],
                    [THURSDAY_TS, FRIDAY_TS - 1],
                    [FRIDAY_TS, SATURDAY_TS - 1],
                    [SATURDAY_TS, NEXT_SUNDAY_TS - 1],
                    [NEXT_SUNDAY_TS, NEXT_NEXT_SUNDAY_TS - 1],
                    [NEXT_NEXT_SUNDAY_TS, NEXT_NEXT_MONDAY_TS - 1],
                    [NEXT_NEXT_MONDAY_TS, NEXT_NEXT_TUESDAY_TS - 1],
                   ], segs
    end
  end
=end
end
