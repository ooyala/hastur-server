require "cassandra"
require "multi_json"
require "date"

module Hastur
  module Cassandra
    extend self

    FIVE_MINUTES = 5 * 60

    def time_segment_for_timestamp(timestamp, granularity = FIVE_MINUTES)
      # :name, :value, :timestamp
      time = Time.at(timestamp / 1_000_000)

      # Timestamp of start of day
      date = time.to_date.to_time.to_i

      # How many seconds we are into the day
      secs_into_day = (time - date).to_i

      time_division = (secs_into_day / granularity).to_i * granularity

      # This is the time, rounded down to the nearest 'granularity' seconds from start of day
      date + time_division
    end

    def row_key(uuid, timestamp, granularity = FIVE_MINUTES)
      time_segment = time_segment_for_timestamp(timestamp, granularity)

      # The row key uses the client ID spelled out in hex, not compressed to 128 bits.
      # However, rows are huge and this makes them easy to understand and query.
      # Similarly, the time_segment is a timestamp in seconds rather than a compressed
      # 64-bit number.
      "#{uuid}-#{time_segment}"
    end

    def col_name(stat, timestamp)
      colname = "#{name}-#{[timestamp].pack("Q>")}"
    end

    CF_FOR_STAT_TYPES = {
      :json => :StatsArchive,
      :counter => :StatsCounter,
      :gauge => :StatsGauge,
      :timer => :StatsTimer,
      :mark => :StatsMark,
    }

    def column_family_for_stat_type(type)
      CF_FOR_STAT_TYPES[type]
    end

    # Options from Twitter Cassandra gem:
    #   :ttl
    #   :consistency
    # Additional options:
    #   :uuid - client UUID
    def insert_stat(cass_client, json_string, options = { :consistency => 2 })
      hash = MultiJson.decode(json_string, :symbolize_keys => true)

      if options.has_key?(:uuid)
        hash[:uuid] = options[:uuid]
        json_string = MultiJson.encode(hash)
      end

      name = hash[:name]
      value = hash[:value]
      timestamp_usec = hash[:timestamp]
      colname = col_name(name, timestamp_usec)

      key = ::Hastur::Cassandra.row_key(hash[:uuid], hash[:timestamp])
      cf = CF_FOR_STAT_TYPES[hash[:type].to_sym]
      raise "Unknown stat type #{hash[:type].inspect}!" unless cf
      cass_client.insert(:StatsArchive, key, { colname => json_string }, options)
      cass_client.insert(cf, key, { colname => value.to_s }, options) unless cf == :StatsArchive
    end

    def get_stat(cass_client, client_uuid, stat, type, start_timestamp, end_timestamp)
      start_ts = time_segment_for_timestamp(start_timestamp)
      end_ts = time_segment_for_timestamp(end_timestamp)

      if (end_ts - start_ts) / FIVE_MINUTES > 288
        raise "Too many time segments!  No more than a day at once."
      end

      segments = [start_ts]
      ts = start_ts
      while ts < end_ts
        ts += FIVE_MINUTES
        segments << ts
      end

      raise "Error calculating time segments (#{segments[0]}-#{segments[-1]} ~ #{end_ts})!" unless segments[-1] == end_ts

      cf = CF_FOR_STAT_TYPES[type]
      raise "No such stat type as #{type}!" unless cf

      start_column = col_name(stat, start_timestamp)
      end_column = col_name(stat, end_timestamp)

      start_row_key = ::Hastur::Cassandra.row_key(client_uuid, start_timestamp)
      end_row_key = ::Hastur::Cassandra.row_key(client_uuid, end_timestamp)
      row_keys = segments.map { |seg| "#{client_uuid}-#{seg}" }

      # For now, be stupid and get back all columns from all rows
      cass_client.multi_get(cf, row_keys, :count => 10_000)
    end

  end
end
