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

    protected

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

    def uuid_from_row_key(row_key)
      row_key.split("-")[0..-2].join("-")
    end

    def col_name_to_stat_and_timestamp(col_name)
      time_packed = col_name[-8...-1]
      timestamp = time_packed.unpack("Q>")

      # Skip col_name[-9], which is the dash between stat name and packed timestamp
      stat = col_name[0..-10]

      [ stat, timestamp ]
    end

    public

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
      uuid = options[:uuid] || hash[:uuid]

      name = hash[:name]
      value = hash[:value]
      timestamp_usec = hash[:timestamp]
      colname = col_name(name, timestamp_usec)

      key = ::Hastur::Cassandra.row_key(uuid, timestamp_usec)
      cf = CF_FOR_STAT_TYPES[hash[:type].to_sym]
      raise "Unknown stat type #{hash[:type].inspect}!" unless cf
      cass_client.insert(:StatsArchive, key, { colname => json_string }, options)
      cass_client.insert(cf, key, { colname => value.to_s }, options) unless cf == :StatsArchive
    end

    protected

    def segments_for_timestamps(start_timestamp, end_timestamp)
      start_ts = time_segment_for_timestamp(start_timestamp)
      end_ts = time_segment_for_timestamp(end_timestamp)

      num_ts = (end_ts - start_ts) / 1_000_000 / FIVE_MINUTES

      segments = [start_ts]
      ts = start_ts
      while ts < end_ts
        ts += FIVE_MINUTES
        segments << ts
      end

      raise "Error calculating time segments!" unless segments[-1] == end_ts

      segments
    end

    CASS_GET_OPTIONS = [ :consistency, :count, :start, :finish, :reversed ]

    def __get_all_stats(cass_client, client_uuid, start_timestamp, end_timestamp, options = {})
      segments = segments_for_timestamps(start_timestamp, end_timestamp)

      type = options[:type] || :json

      cf = CF_FOR_STAT_TYPES[type]
      raise "No such stat type as #{type}!" unless cf

      row_keys = segments.map { |seg| "#{client_uuid}-#{seg}" }

      cass_options = { :count => 10_000 }
      CASS_GET_OPTIONS.each do |opt|
        cass_options[opt] = options[opt] if options.has_key?(opt)
      end
      values = cass_client.multi_get(cf, row_keys, cass_options)

      # Delete empty rows
      values.delete_if { |_, value| value.nil? || value.empty? }

      final_values = {}
      values.each do |row_key, col_hash|
        col_hash.each do |col_key, value|
          stat, timestamp = col_name_to_stat_and_timestamp(col_key)

          final_values[stat] ||= {}
          final_values[stat][timestamp] = value
        end
      end

      final_values
    end

    public

    HOURS = 60 * 60

    def get_stat(cass_client, client_uuid, stat, type, start_timestamp, end_timestamp, options = {})
      if (end_timestamp - start_timestamp) > 72 * HOURS
        raise "Don't query more than 3 days at once yet!"
      end

      start_column = col_name(stat, start_timestamp)
      end_column = col_name(stat, end_timestamp)

      __get_all_stats(cass_client, client_uuid, start_timestamp, end_timestamp,
                      options.merge(:start => start_column, :finish => end_column, :type => type))
    end

    def get_all_stats(cass_client, client_uuid, start_timestamp, end_timestamp, options = {})
      if (end_timestamp - start_timestamp) / 1_000_000.0 > 72 * HOURS
        raise "Don't query more than 3 days at once yet!"
      end

      values = __get_all_stats(cass_client, client_uuid, start_timestamp, end_timestamp,
                               options.merge(:type => :json))

      # TODO(noah): Filter by timestamp
    end
  end
end
