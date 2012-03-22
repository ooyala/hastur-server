require "hastur-server/sink/cassandra_schema"

module Hastur
  module Cassandra
    extend self

    NAME_BY_GRANULARITY = {
      FIVE_MINUTES => "FiveMinute",
      ONE_HOUR => "Hour",
      ONE_DAY => "Day",
      ONE_WEEK => "Week",
      # Month and year are weird - they aren't the same kind of simple granularities
    }

    # For now, leave out month and year to keep the math simple. These are all Fixnums
    GRANULARITIES = [ONE_WEEK, ONE_DAY, ONE_HOUR, FIVE_MINUTES]

    #
    # Given a start time, returns the timestamp for the previous occuring rollup for a particular granularity
    #
    def last_time_segment_for_timestamp(start_ts, granularity)
      # For example: 
      #   remainder = start_ts % granularity
      #       1     =    10    %      3
      #   start_ts - remainder = last_time_segment
      #       10   -     1     =       9

      # do not combine these two statements because it will be difficult to debug later on
      remainder = start_ts % granularity
      start_ts - remainder
    end

    #
    # Given a start time, returns the timestamp for the next occuring rollup for a particular granularity
    #
    def next_time_segment_for_timestamp(start_ts, granularity)
      # For example: 
      #   remainder = start_ts % granularity
      #       1     =     10   %       3
      #   start_ts - remainder = last_time_segment
      #       10   -     1     =        9
      #   next_time_segment = last_time_segment + granularity
      #       12            =        9          +      3

      # do not combine these two statements because it will be difficult to debug later on
      remainder = start_ts % granularity
      start_ts - remainder + granularity
    end

    #
    # Retrieves an OrderedHash for the next rollup
    #
    def get_next_rollup(cass_client, route, timestamp, granularity)
      rollup_timestamp = next_time_segment_for_timestamp(timestamp, granularity)
      # get the CF from route
      raise "Unable to determine find schema for #{route}" unless SCHEMA[route]
      rollup_cf = SCHEMA[route][:rollup_cf]
      raise "Unable to determine the rollup column family for #{route}" unless rollup_cf
      # get the rollup
      cass_client.get(rollup_cf, rollup_timestamp.to_s)
    end

    #
    # Retrieves an OrderedHash for the previous rollup
    #
    def get_previous_rollup(cass_client, route, timestamp, granularity)
      rollup_timestamp = last_time_segment_for_timestamp(timestamp, granularity)
      # get the CF from route
      raise "Unable to determine find schema for #{route}" unless SCHEMA[route]
      rollup_cf = SCHEMA[route][:rollup_cf]
      raise "Unable to determine the rollup column family for #{route}" unless rollup_cf
      # get the rollup
      cass_client.get(rollup_cf, rollup_timestamp.to_s)
    end

    #
    # Writes a value in the next rollup for a route
    #
    def write_rollup(cass_client, route, timestamp, granularity, col, value)
      rollup_timestamp = next_time_segment_for_timestamp(timestamp, granularity)
      # get the CF from route
      raise "Unable to determine find schema for #{route}" unless SCHEMA[route]
      rollup_cf = SCHEMA[route][:rollup_cf]
      raise "Unable to determine the rollup column family for #{route}" unless rollup_cf
      cass_client.insert(rollup_cf, rollup_timestamp.to_s, {col => value.to_s })
    end

    #
    # Writes a rolled-over rollup from the previous segment to the next.
    #
    # @params [Cassandra] cass_client Cassandra client
    # @params [String] route Hastur ZMQ route used to know which CF to write to
    # @params [Fixnum] timestamp Hastur time in usecs. Used to know which time segment to write to
    # @params [Fixnum] granularity Number of usecs for a time segment. Must be a valid granularity from 
    #                              Hastur::Cassandra::GRANULARITIES
    # @params [OrderedHash] ordered_hash The data is that rolling over from the period segment.
    #
    def write_ordered_hash_rollup(cass_client, route, timestamp, granularity, ordered_hash)
      ordered_hash.keys.each do |key|
        write_rollup(cass_client, route, timestamp, granulairty, key, ordered_hash[key])
      end
    end

    #
    # UNTESTED
    #
    # For messages with multi-granularity rollups (i.e. hours, days,
    # weeks) we need to be able to take a chunk of time and divide it
    # into pieces of granularity sizes.  For instance, a nine-day span
    # might turn into a single one-week chunk and two one-day chunks
    # on either side.
    #
    # @param [Fixnum] start_ts The beginning timestamp of the time interval
    # @param [Fixnum] end_ts The end timestamp of the time interval
    # @param [Array] granularities The list of granularities to break the interval into
    #
    def get_granular_segments_from_timestamps(start_ts, end_ts, granularities = GRANULARITIES)
      return [] if granularities.empty? || (end_ts - start_ts) > granularities[0]

      # Start with the largest granularity - a week, at the top level
      granularity = granularities[0]

      # Get the first whole chunk (e.g. week) that the first timestamp overlaps
      first_segment = time_segment_for_timestamp(start_ts, granularity)

      # Now, get the first whole chunk that starts *after* start_ts
      first_segment += granularity if start_ts > first_segment

      # Now, how many whole chunks this size can we go forward before we're done?
      segments = time_segments_for_timestamps(first_segment, end_ts, granularity)

      # Remove the last chunk if it extends past the end timestamp
      segments.pop if (segments[-1] + granularity - 1) > end_ts

      # Now we have the whole list of all biggest-size chunks wholly contained in the times.
      # Convert to intervals so it's clearer what's going on and we know the granularities.
      segments = segments.map { |s| [ s, s + granularity - 1] }

      # Now, grab all smaller chunks before the start of the first big chunk, and all smaller
      # chunks after the end of the last big chunk.  Then return everything.
      get_time_segments_from_timestamps(start_ts, segments[0][0] - 1, granularities[1..-1]) +
        segments +
        get_time_segments_from_timestamps(segments[-1][1] + 1, segments[0] * 1_000_000, granularities[1..-1])
    end

    #
    # Get the list of cassandra queries to find this time range of UUIDs
    #
    def get_uuid_cass_queries_over_time(start_ts, end_ts, options = {})
      # TODO(noah) - consider expanding opportunistically to return larger-granularity buckets instead
      # of multiple smaller ones.  UUIDs in particular will have a lot of repetition in the small
      # buckets.

      # Make sure we return all relevant UUIDs - for now, just expand five minutes in either direction
      first_segment = time_segment_for_timestamp(start_ts - FIVE_MINUTES, ONE_DAY)
      segments = segments_for_timestamps(first_segment, end_ts + FIVE_MINUTES, ONE_DAY)

      cass_queries = segments.map do |seg_start_ts|
        [ :UUIDDay, seg_start_ts.to_s ]
      end
    end

    #
    # Get the list of cassandra queries to find this time range of stat names.
    #
    def get_stat_name_cass_queries_over_time(start_ts, end_ts, options = {})
      # Make sure we return all relevant stat names - for now, just expand five minutes in either direction

      first_segment = time_segment_for_timestamp(start_ts - FIVE_MINUTES, ONE_DAY)
      segments = segments_for_timestamps(first_segment, end_ts + FIVE_MINUTES, ONE_DAY)
      cass_queries = segments.map do |seg_start_ts|
        [ :StatNameDay, seg_start_ts.to_s ]
      end
    end

    #
    # Convert a cassandra query list to the actual data from the queries.
    #
    def cass_queries_to_data(cass_client, queries, options = { :count => 10_000, :consistency => 1 })
      queries_by_cf = {}

      queries.each do |cf, row_key|
        queries_by_cf[cf] ||= []
        queries_by_cf[cf] << row_key
      end

      values = {}
      queries_by_cf.each do |cf, rows|
        values.merge! cass_client.multi_get(cf, rows, options)
      end

      values
    end

  end
end
