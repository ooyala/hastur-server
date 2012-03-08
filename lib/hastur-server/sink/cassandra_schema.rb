require "cassandra"
require "msgpack"
require "multi_json"
require "date"

require "hastur-server/util"

# Data types we will eventually want to support:
#
#
# Stats - column families by subtype, five-minute resolution, high traffic
# Logs - subdivision?  High resolution.  High traffic.
# Errors - no subdivision, medium resolution?, medium traffic?
# Rawdata - large entries, low resolution, low number of entries but sometimes large
# Notification - no subdivision, low resolution, low traffic
# Heartbeat - subdivision?, high resolution, high traffic
# Registration - no subdivision, low resolution, low traffic

# Needs more study:
# Plugin results - subdivision?, high resolution, high traffic - like stats?

module Hastur
  module Cassandra
    extend self

    # These constants aren't so much "public" as they are useful
    # in-module and we don't care about securing them.

    FIVE_MINUTES = 5 * 60 * 1_000_000
    CASS_GET_OPTIONS = [ :consistency, :count, :start, :finish, :reversed ]
    ONE_HOUR = 12 * FIVE_MINUTES
    HOURS = ONE_HOUR
    ONE_DAY = 24 * HOURS
    ONE_WEEK = 7 * ONE_DAY

    SCHEMA = {
      "stat" => {
        :cf => :StatArchive,
        :subtype => {
          :type => {
            :cf => {
              :gauge => :StatGauge,
              :counter => :StatCounter,
              :mark => :StatMark,
            },
            :value => {
              :gauge => :value,
              :counter => :increment,
              :mark => nil,
            },
            :rollup_cf_prefix => {
              :gauge => "StatGauge",
              :counter => "StatCounter",
              :mark => "StatMark",
            }
          }
        },
        :granularity => FIVE_MINUTES,
        :name => :name,
        :name_cf => :StatNameDay,
      },
      "log" => {
        :cf => :LogArchive,
        :granularity => FIVE_MINUTES,
        :name => nil,
      },
      "error" => {
        :cf => :ErrorArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
      "rawdata" => {
        :cf => :RawdataArchive,
        :granularity => ONE_DAY,  # Yes?  No?
        :name => nil,
      },
      "event" => {
        :cf => :EventArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
      "heartbeat" => {
        :cf => :HeartbeatArchive,
        :granularity => FIVE_MINUTES,
        :name => :name,
        :rollup_cf_prefix => "Heartbeat",
      },
      # No plugin_exec - not for sinks
      "registration" => {
        :cf => :RegistrationArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
    }

    # Options from Twitter Cassandra gem:
    #   :ttl
    #   :consistency
    # Additional options:
    #   :uuid - client UUID
    def insert_stat(cass_client, json_string, options = {})
      insert(cass_client, json_string, "stat", options)
    end

    # Options from Twitter Cassandra gem:
    #   :ttl
    #   :consistency
    # Additional options:
    #   :uuid - client UUID
    #   :route - sink sent to (required)
    def insert(cass_client, json_string, route, options = {})
      hash = MultiJson.decode(json_string, :symbolize_keys => true)
      raise "Cannot deserialize JSON string!" unless hash
      uuid = options.delete(:uuid) || hash[:uuid] || hash[:from]
      raise "No UUID given!" unless uuid

      schema = SCHEMA[route]
      raise "No schema defined for route #{route}!" unless schema

      subdivide = false
      if schema[:subtype]
        subdivide = true
        sub_key = schema[:subtype].keys[0]   # Example: :type
        type = hash[sub_key].to_sym            # Example: :gauge
        raise "No '#{sub_key}' specified in the payload" unless type

        subtype = schema[:subtype][sub_key]
        raise "Unknown #{route} #{sub_key}: #{type.inspect}!" unless subtype

        cf = subtype[:cf][type]                # Example: :StatGauge

        value_name = subtype[:value][type]     # Example: :value
        value = hash[value_name]               # Example: 37.914
      end

      name = hash[:name]
      timestamp_usec = hash[:timestamp]

      colname = col_name(name, timestamp_usec)
      key = ::Hastur::Cassandra.row_key(uuid, timestamp_usec, schema[:granularity] || ONE_DAY)
      one_day_ts = time_segment_for_timestamp(timestamp_usec, ONE_DAY)

      insert_options = { }
      insert_options[:consistency] = options[:consistency] if options[:consistency]
      now_ts = ::Hastur::Util.timestamp.to_s
      cass_client.batch do |client|
        client.insert(schema[:cf], key, { colname => json_string,
                        "last_write" => now_ts, "last_access" => now_ts }, insert_options)
        client.insert(cf, key, { colname => value.to_msgpack, "last_write" => now_ts,
                        "last_access" => now_ts }, insert_options) if subdivide

        # Insert into "saw this in this time period" rows
        client.insert(:UUIDDay, one_day_ts.to_s, { uuid => "" })
        if schema[:name_cf]
          client.insert(schema[:name_cf], one_day_ts.to_s, { name => "" })
        end
      end
    end

    def get(cass_client, client_uuid, route, start_timestamp, end_timestamp, options = {})
      if (end_timestamp - start_timestamp) / 1_000_000.0 > 72 * HOURS
        raise "Don't query more than 3 days at once yet!"
      end

      raw_get_all(cass_client, client_uuid, route, start_timestamp, end_timestamp, options)
    end

    # Get a stat on a given client UUID over a given block of time, up to about a day.
    # If a :type option is given, pull the values from that type's storage area.  Otherwise,
    # pull raw JSON information from the all-stats archive area.
    #
    # Options:
    #   :consistency - Cassandra read consistency
    #   :count - maximum number of entries to return, default 10000
    #
    def get_stat(cass_client, client_uuid, stat_name, type, start_timestamp, end_timestamp, options = {})
      if (end_timestamp - start_timestamp) > 72 * HOURS
        raise "Don't query more than 3 days at once yet!"
      end

      raw_get_all(cass_client, client_uuid, "stat", start_timestamp, end_timestamp,
                  options.merge(:name => stat_name, :subtype => type))
    end

    # Get all stats on a given client UUID over a given block of time, up to about a day.
    # If a :type option is given, pull the values from that type's storage area.  Otherwise,
    # pull raw JSON information from the all-stats archive area.
    #
    # Options:
    #   :type - :gauge, :mark, :counter (nil for raw)
    #   :consistency - Cassandra read consistency
    #   :count - maximum number of entries to return, default 10000
    #
    def get_all_stats(cass_client, client_uuid, start_timestamp, end_timestamp, options = {})
      if (end_timestamp - start_timestamp) / 1_000_000.0 > 72 * HOURS
        raise "Don't query more than 3 days at once yet!"
      end

      if [ :gauge, :mark, :counter ].include?(options[:type])
        options[:subtype] = options[:type]
      elsif options[:type]
        raise "Unknown type #{options[:type]} passed to get_all_stats!"
      end

      raw_get_all(cass_client, client_uuid, "stat", start_timestamp, end_timestamp, options)
    end

    protected

    def time_segment_for_timestamp(timestamp, granularity)
      # :name, :value, :timestamp
      time = Time.at(timestamp / 1_000_000)

      date = time.to_date

      if granularity == ONE_WEEK
        one_day_seconds = 24 * 60 * 60
        start_of_week = date - date.wday * one_day_seconds   # In sec, not usec
        return Time.utc(start_of_week.year, start_of_week.month, start_of_week.day).to_i * 1_000_000
      end

      # Timestamp in seconds of start of day
      date_secs = Time.utc(date.year, date.month, date.day).to_i

      # How many seconds we are into the day in UTC
      usecs_into_day = (time - date_secs).to_i * 1_000_000

      chunks_into_day = (usecs_into_day / granularity).to_i
      time_division = chunks_into_day * granularity

      # This is the time, rounded down to the nearest 'granularity' seconds from start of day
      date_secs * 1_000_000 + time_division
    end

    def row_key(uuid, timestamp, granularity)
      time_segment = time_segment_for_timestamp(timestamp, granularity)

      # The row key uses the client ID spelled out in hex, not compressed to 128 bits.
      # However, rows are huge and this makes them easy to understand and query.
      # Similarly, the time_segment is a timestamp in seconds rather than a compressed
      # 64-bit number.
      "#{uuid}-#{time_segment}"
    end

    def col_name(name, timestamp)
      if name
        colname = "#{name}-#{[timestamp].pack("Q>")}"
      else
        colname = [timestamp].pack("Q>")
      end
    end

    def uuid_from_row_key(row_key)
      row_key.split("-")[0..-2].join("-")
    end

    def col_name_to_name_and_timestamp(col_name)
      time_packed = col_name[-8..-1]
      timestamp = time_packed.unpack("Q>")[0]

      # Skip col_name[-9], which is the dash between name and packed timestamp
      name = col_name[0..-10]

      [ name, timestamp ]
    end

    #
    # This calculates the set of time segments of the given granularity that
    # overlap the given range of timestamps.  Commonly the given timestamps
    # won't be on segment boundaries which will result in the segments
    # covering a larger range of timestamps.  This is expected behavior.
    #
    def segments_for_timestamps(start_timestamp, end_timestamp, granularity)
      start_ts = time_segment_for_timestamp(start_timestamp, granularity)
      end_ts = time_segment_for_timestamp(end_timestamp, granularity)

      segments = [start_ts]
      ts = start_ts
      while ts < end_ts
        ts += granularity
        segments << ts
      end

      segments
    end

    #
    # This is the basic getter for messages.  By default it gets all messages with a given
    # route and client UUID across the given timestamps.  It can be modified in several
    # other ways by options:
    #
    # Hastur Options:
    #   :name - the message name such as stat name, heartbeat name or plugin name
    #   :subtype - message subtype such as :counter for stats (nil for none)
    #
    # Cassandra Options:
    #   :count - maximum number of messages, default 10,000
    #   :consistency - read consistency
    #   :start - starting column name in each row
    #   :finish - final column name in each row
    #   :reversed - return results in reverse order
    #
    # You can specify :name or :start/:finish, but not both.
    # If you specify :name, it will be implemented by changing the
    # :start and :finish options to Cassandra.
    #
    def raw_get_all(cass_client, client_uuid, route, start_timestamp, end_timestamp, options = {})
      if options[:name] && (options[:start] || options[:finish])
        raise "Error: you can't specify the :name option with :start or :finish in raw_get_all!"
      end

      schema = SCHEMA[route]
      raise "No schema for route #{route}!" unless schema

      name_field = schema[:name]
      cf = schema[:cf]
      granularity = schema[:granularity]

      subdivide = false
      if schema[:subtype] && options[:subtype]
        subdivide = true
        sub_key = schema[:subtype].keys[0]     # Example: :type

        subtype = schema[:subtype][sub_key]

        cf = subtype[:cf][options[:subtype]]   # Example: :StatGauge
        raise "No such subtype as #{options[:subtype]}!" unless cf
        value_name = subtype[:value][options[:subtype]] # Example: :value
        # value_name may be nil, so don't raise on nil
      end

      segments = segments_for_timestamps(start_timestamp, end_timestamp, granularity)

      if client_uuid.kind_of?(Array)
        row_keys = client_uuid.map do |uuid|
          segments.map { |seg| "#{uuid}-#{seg}" }
        end.flatten
      else
        row_keys = segments.map { |seg| "#{client_uuid}-#{seg}" }
      end

      cass_options = { :count => 10_000 }
      CASS_GET_OPTIONS.each do |opt|
        cass_options[opt] = options[opt] if options.has_key?(opt)
      end

      if name_field && options[:name]
        # For a named schema like stats or heartbeats, tell Cassandra what column range to query.
        cass_options[:start] = col_name(options[:name], start_timestamp)
        cass_options[:finish] = col_name(options[:name], end_timestamp)
      elsif !name_field
        # For an unnamed schema like errors, tell Cassandra what column range to query
        cass_options[:start] = col_name(nil, start_timestamp) unless options[:start]
        cass_options[:finish] = col_name(nil, end_timestamp) unless options[:finish]
      end

      # Now, actually do the query
      values = cass_client.multi_get(cf, row_keys, cass_options)

      # Mark rows as accessed
      now_ts = Hastur::Util.timestamp(nil)
      cass_client.batch do |client|
        row_keys.each do |key|
          client.insert(cf, key, "last_access" => now_ts.to_s)
        end
      end

      # Delete empty rows in result
      values.delete_if { |_, value| value.nil? || value.empty? }

      final_values = {}
      values.each do |row_key, col_hash|
        col_hash.each do |col_key, value|
          name, timestamp = col_name_to_name_and_timestamp(col_key)

          if timestamp <= end_timestamp && timestamp >= start_timestamp
            final_values[name] ||= {}
            final_values[name][timestamp] = subdivide ? MessagePack.unpack(value) : value
          end
        end
      end

      final_values
    end
  end
end
