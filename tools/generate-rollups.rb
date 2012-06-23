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

      # row keys look like: e89a815a-9af5-11e1-83f1-22000a1cbdc8-1339605900000000
      # col keys look like: hastur.agent.utime-\x00\x04\xc1\x0cY\xe6\x811

      def check_db_rollups(cass_client, end_ts, span)
        %w[gauge counter].each do |type|
          start_ts = end_ts - span
          #uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
          uuids = { "6bbaffa0-7140-012f-1b93-001e6713f84b" => "" }
          schema = Hastur::Cassandra.schema_by_type type
          buckets = usec_aligned_chunks(start_ts, end_ts, schema[:granularity])

          uuids.keys.each do |uuid|
            hourly = {}
            daily = {}
            buckets.each do |bucket|
              rowkey = [uuid, bucket].join('-')
              row = cass_client.get schema[:values_cf], rowkey, :consistency => 5 # 5 => ALL

              # TODO(al) finalize the write buckets, this is just a guess, need to talk it over with the team
              # It probably makes sense to go with day / week / month or week / month / year with month / year
              # defined as midnight of 1st of <period> rather than the usual static number of usecs.
              # Or just cram everything into a week bucket and keep it simple. 52 buckets to get a year of data
              # isn't terrible.
              write_bucket = usec_truncate(bucket, :one_week)

              next unless row and row.keys.any?

              if schema[:granularity] == USEC_FIVE_MINUTES
                rollup = compute(row)
                rollup[:interval] = USEC_FIVE_MINUTES
                cass_client.insert schema[:rollup_cf], "#{uuid}-five_minutes-#{write_bucket}", encode(rollup, bucket)
              end

              hourly[rowkey] = row
              daily[rowkey] = row

              if schema[:granularity] < USEC_ONE_HOUR and bucket == usec_truncate(bucket, :one_hour)
                rollup = compute(merge_rows(hourly))
                rollup[:interval] = USEC_ONE_HOUR
                row_bucket = usec_truncate(bucket, :one_week)
                cass_client.insert schema[:rollup_cf], "#{uuid}-one_hour-#{write_bucket}", encode(rollup, bucket)
                hourly = {}
              end

              if schema[:granularity] < USEC_ONE_DAY and bucket == usec_truncate(bucket, :one_day)
                rollup = compute(merge_rows(daily))
                rollup[:interval] = USEC_ONE_DAY
                cass_client.insert schema[:rollup_cf], "#{uuid}-one_day-#{write_bucket}", encode(rollup, bucket)
                daily = {}
              end
            end
            # fall off and don't compute incomplete hour/day buckets
          end
        end
      end

      def encode(rollup, bucket)
        out = {}
        rollup.each do |key, values|
          packed = MessagePack.pack(values)
          colkey = "#{key}-#{[bucket].pack('Q>')}"
          out[colkey] = packed
        end
        out
      end

      def merge_rows(source)
        out = {}
        source.values.each do |row|
          out.merge! row
        end
        out
      end

      def compute(row)
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

        # TODO: max / min
        rollup.each do |key, col_rollup|
          # both lists need to be in order, there's no need to maintain the tuples
          values = col_rollup.delete(:values).sort
          timestamps = col_rollup.delete(:timestamps).sort

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
            rank = (col_rollup[:count] * (percentile / 100) + 0.5).round
            col_rollup["p#{percentile}"] = values[rank]
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

      # http://en.wikipedia.org/wiki/Standard_deviation
      def stddev(list)
        dsum = 0
        avg = list.reduce(:+) / list.count
        list.each do |v|
          diff = v - avg
          dsum += diff * diff
        end

        variance = dsum / list.count

        return Math.sqrt(variance), variance, avg
      end

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

last_ts = Hastur::TimeUtil.usec_truncate Hastur::TimeUtil.usec_epoch, :five_minutes

Hastur::Cassandra::Rollup.check_db_rollups(cass_client, last_ts, Hastur::TimeUtil::USEC_ONE_DAY)

