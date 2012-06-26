#!/usr/bin/env ruby

require "cassandra/1.0"
require "cassandra/constants"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "msgpack"
require "termite"
require "trollop"
require "time"

# TODO(al) make this into a regular module so it can do on-the-fly rollups for the retrieval service
# TODO(al) rollup up composite columns, probably in a separate program
# TODO(al) error handling
# TODO(al) test test test test test!

module Hastur
  module Cassandra
    module Rollup
      include Hastur::TimeUtil
      extend self

      #
      # Generate rollups for the given CF in the provided time range.
      #
      # @param [String] archive which Archive CF to roll up, e.g. 'gauge', 'counter'
      # @param [Fixnum] start_ts beginning of the period to roll up
      # @param [Fixnum] end_ts end of the period to roll up
      # @example
      #   end_ts = Hastur::TimeUtil.epoch_usec
      #   start_ts = end_ts - Hastur::TimeUtil::USEC_ONE_DAY
      #   Hastur::Cassandra::Rollup.rollup cass_client, 'gauge', start_ts, end_ts
      #
      def rollup(cass_client, archive, start_ts, end_ts)
        #uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
        uuids = { "6bbaffa0-7140-012f-1b93-001e6713f84b" => "" }
        schema = Hastur::Cassandra.schema_by_type archive
        buckets = usec_aligned_chunks(start_ts, end_ts, schema[:granularity])

        uuids.keys.each do |uuid|
          hourly = {}
          daily = {}
          buckets.each do |bucket|
            rowkey = [uuid, bucket].join('-')
            row = cass_client.get schema[:values_cf], rowkey, :consistency => 5 # 5 => ALL

            # write all the rollups < 1 week into a weekly bucket since it's the largest time interval with a
            # (mostly) predictable size (7 days * 24 hours * 3600 seconds)
            week_bucket = usec_truncate(bucket, :one_week)

            # skip empty rows, shouldn't be many, but it can happen
            next unless row and row.keys.any?

            # gauge/counter are bucketed at 5 minutes and will always want 5 minute rollups
            # while there aren't currently any < 5min buckets, it's feasible so that's supported too
            if schema[:granularity] <= USEC_FIVE_MINUTES and bucket == usec_truncate(bucket, :five_minutes)
              rollup = compute(row, USEC_FIVE_MINUTES)
              cass_client.insert schema[:rollup_cf], "#{uuid}-five_minutes-#{week_bucket}", encode(rollup, bucket)
            end

            # build up super-rows for hour / day rollups, larger rollups may choose to aggregate 5min rollups
            # to avoid having to load huge rows into memory somewhere
            hourly[rowkey] = row
            daily[rowkey] = row

            # build hourly rollups if the granularity is smaller than that and we've hit an hour boundary
            if schema[:granularity] <= USEC_ONE_HOUR and bucket == usec_truncate(bucket, :one_hour)
              rollup = compute(merge_rows(hourly), USEC_ONE_HOUR)
              row_bucket = usec_truncate(bucket, :one_week)
              cass_client.insert schema[:rollup_cf], "#{uuid}-one_hour-#{week_bucket}", encode(rollup, bucket)
              hourly = {}
            end

            # same thing, but for a day
            if schema[:granularity] <= USEC_ONE_DAY and bucket == usec_truncate(bucket, :one_day)
              rollup = compute(merge_rows(daily), USEC_ONE_DAY)
              cass_client.insert schema[:rollup_cf], "#{uuid}-one_day-#{week_bucket}", encode(rollup, bucket)
              daily = {}
            end
          end
          # fall off and don't compute incomplete hour/day buckets
        end
      end

      private

      #
      # pack in the same messagepack format used in Hastur::Cassandra::Schema
      #
      # @param [Hash{String => Object}] columns columns to pack & rekey
      # @param [Fixnum] bucket_ts bucket timestamp for column key
      # @return [Hash{String => Object}] encoded/keyed hash ready for insert
      #
      def encode(columns, bucket_ts)
        out = {}
        columns.each do |key, values|
          packed = MessagePack.pack(values)
          colkey = "#{key}-#{[bucket_ts].pack('Q>')}"
          out[colkey] = packed
        end
        out
      end

      # simple helper for merging a hash of rows into a single large row
      def merge_rows(source)
        out = {}
        source.values.each do |row|
          out.merge! row
        end
        out
      end

      #
      # Compute a rollup from the given "row" of data.
      #
      # @param [Hash{String => String}] row data row to roll up
      # @param [Fixnum] bucket_interval time length the rollup is supposed to cover
      # @return [Hash{String => Hash{Symbol => Numeric}}]
      #
      def compute(row, bucket_interval)
        rollup = {}

        row.each do |col, packval|
          next if col =~ /collectd/
          val = MessagePack.unpack packval
          # skip any non-numeric entries since we can't really make sense of them
          next unless val.kind_of? Numeric

          key, timestamp = init(rollup, col)

          rollup[key][:values] << val # will be deleted
          rollup[key][:timestamps] << timestamp # will be deleted
        end

        rollup.each do |key, col_rollup|
          # both lists need to be in order, there's no need to maintain the tuples
          values = col_rollup.delete(:values).sort
          timestamps = col_rollup.delete(:timestamps).sort

          col_rollup[:interval] = bucket_interval
          col_rollup[:min]      = values[0]
          col_rollup[:max]      = values[-1]
          col_rollup[:range]    = values[-1] - values[0]
          col_rollup[:sum]      = values.reduce(:+)
          col_rollup[:count]    = values.count
          col_rollup[:first_ts] = timestamps[0]
          col_rollup[:last_ts]  = timestamps[-1]
          col_rollup[:elapsed]  = timestamps[0] - timestamps[-1]

          # http://en.wikipedia.org/wiki/Percentiles
          # median is just p50
          last = values.count - 1
          [10, 25, 50, 75, 90, 95, 99].each do |percentile|
            rank = (col_rollup[:count] * (percentile / 100.0) + 0.5).round
            col_rollup["p#{percentile}".to_sym] = values[rank]
          end

          # compute the variance & standard deviation
          stddev, variance, average = stddev(values)
          col_rollup[:stddev]   = stddev
          col_rollup[:variance] = variance
          col_rollup[:average]  = average

          # period standard deviation (quality)
          if timestamps.count > 1
            # convert the timestamps to a list of intervals
            last_ts = timestamps.shift
            intervals = []
            timestamps.each do |ts|
              intervals << ts - last_ts
              last_ts = ts
            end

            stddev, variance, average = stddev(intervals)
            col_rollup[:period] = average
            col_rollup[:jitter] = stddev
          end
        end

        return rollup
      end

      #
      # http://en.wikipedia.org/wiki/Standard_deviation
      #
      # @param [Array<Numeric>] list of values
      # @return [Float, Float, Float] stddev, variance, average
      #
      def stddev(list)
        avg = list.reduce(&:+) / list.count.to_f
        dsum = list.map { |v| (v - avg) ** 2 }.reduce(&:+)
        variance = dsum / list.count.to_f
        return Math.sqrt(variance), variance, avg
      end

      #
      # Unpack the column key and initialize the rollup slot if necessary.
      #
      # @param [Hash{String => Hash}] rollup
      # @param [String] colkey still-encoded column key
      #
      def init(rollup, colkey)
        key, _, timestamp = colkey.unpack("a#{colkey.bytesize - 9}aQ>")
        rollup[key] ||= {
          :count      => 0,
          :sum        => 0,
          :average    => 0,
          :period     => 0,
          :first_ts   => nil,
          :last_ts    => nil,
          :values     => [],
          :timestamps => [],
        }
        return key, timestamp
      end
    end
  end
end

opts = Trollop::options do
  opt :cassandra, "Cassandra server list", :default => ["127.0.0.1:9202"], :type => :strings, :multi => true
  opt :keyspace, "Cassandra Keyspace to use", :default => "Hastur"
end

cass_client = ::Cassandra.new(opts[:keyspace], opts[:cassandra].flatten)
cass_client.disable_node_auto_discovery!

#last_ts = Hastur::TimeUtil.usec_truncate Hastur::TimeUtil.usec_epoch, :five_minutes
#Hastur::Cassandra::Rollup.check_db_rollups(cass_client, last_ts, Hastur::TimeUtil::USEC_ONE_DAY)

end_ts = Hastur::TimeUtil.usec_epoch
start_ts = end_ts - Hastur::TimeUtil::USEC_ONE_DAY
Hastur::Cassandra::Rollup.rollup cass_client, 'gauge', start_ts, end_ts
