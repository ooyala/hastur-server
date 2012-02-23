require "cassandra"
require "msgpack"
require "multi_json"

require "date"

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

    FIVE_MINUTES = 5 * 60
    CASS_GET_OPTIONS = [ :consistency, :count, :start, :finish, :reversed ]
    HOURS = 60 * 60
    ONE_DAY = 24 * HOURS

    # TODO(noah): support 'nameless' services like errors and logs.
    # Also maybe support not-stored-by-UUID services (registration?)

    SCHEMA = {
      "stat" => {
        :cf => :StatsArchive,
        :subtype => {
          :type => {
            :cf => {
              :gauge => :StatsGauge,
              :counter => :StatsCounter,
              :mark => :StatsMark,
            },
            :value => {
              :gauge => :value,
              :counter => :increment,
              :mark => nil,
            }
          }
        },
        :granularity => FIVE_MINUTES,
        :name => :name,
      },
      "log" => {
        :cf => :LogsArchive,
        :granularity => FIVE_MINUTES,
        :name => nil,
      },
      "error" => {
        :cf => :ErrorsArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
      "rawdata" => {
        :cf => :RawdataArchive,
        :granularity => ONE_DAY,  # Yes?  No?
        :name => nil,
      },
      "notification" => {
        :cf => :NotificationsArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
      "heartbeat_client" => {
        :cf => :HeartbeatClientsArchive,
        :granularity => FIVE_MINUTES,
        :name => :name,
      },
      "heartbeat_service" => {
        :cf => :HeartbeatServicesArchive,
        :granularity => FIVE_MINUTES,
        :name => :name,
      },
      # No plugin_exec - not for sinks
      "plugin_result" => {
        :cf => :PluginResultsArchive,
        :granularity => FIVE_MINUTES,
        :name => :name,    # Is "name" what we call the plugin name?
      },
      "register_client" => {
        :cf => :RegisterClientsArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
      "register_plugin" => {
        :cf => :RegisterPluginsArchive,
        :granularity => ONE_DAY,
        :name => nil,
      },
      "register_service" => {
        :cf => :RegisterServicesArchive,
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
      insert(cass_client, json_string, options.merge(:route => "stat"))
    end

    # Options from Twitter Cassandra gem:
    #   :ttl
    #   :consistency
    # Additional options:
    #   :uuid - client UUID
    #   :route - sink sent to (required)
    def insert(cass_client, json_string, options = {})
      route = options[:route]
      raise "No :route given!" unless route

      hash = MultiJson.decode(json_string, :symbolize_keys => true)
      raise "Cannot deserialize JSON string!" unless hash
      uuid = options.delete(:uuid) || hash[:uuid]
      raise "No UUID given!" unless uuid

      schema = SCHEMA[route]
      raise "No schema defined for route #{route}!" unless schema

      subdivide = false
      if schema[:subtype]
        subdivide = true
        sub_key = schema[:subtype].keys[0]   # Example: :type
        type = hash[sub_key].to_sym            # Example: :gauge

        subtype = schema[:subtype][sub_key]
        raise "Unknown #{route} #{sub_key}: #{type.inspect}!" unless subtype

        cf = subtype[:cf][type]                # Example: :StatsGauge

        value_name = subtype[:value][type]     # Example: :value
        value = hash[value_name]               # Example: 37.914
      end

      name = hash[:name]
      timestamp_usec = hash[:timestamp]

      colname = col_name(name, timestamp_usec)
      key = ::Hastur::Cassandra.row_key(uuid, timestamp_usec, schema[:granularity] || ONE_DAY)

      insert_options = { }  # TODO(noah): Other cassandra options?
      insert_options[:consistency] = options[:consistency] if options[:consistency]
      cass_client.insert(schema[:cf], key, { colname => json_string }, insert_options)
      cass_client.insert(cf, key, { colname => value.to_msgpack }, insert_options) if subdivide
    end

    def get(cass_client, client_uuid, route, start_timestamp, end_timestamp, options = {})
      if (end_timestamp - start_timestamp) > 72 * HOURS
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

    def time_segment_for_timestamp(timestamp, granularity = FIVE_MINUTES)
      # :name, :value, :timestamp
      time = Time.at(timestamp / 1_000_000)

      # Timestamp of start of day
      date = time.to_date
      date_secs = Time.utc(date.year, date.month, date.day).to_i

      # How many seconds we are into the day in UTC
      secs_into_day = (time - date_secs).to_i

      time_division = (secs_into_day / granularity).to_i * granularity

      # This is the time, rounded down to the nearest 'granularity' seconds from start of day
      date_secs + time_division
    end

    def row_key(uuid, timestamp, granularity = FIVE_MINUTES)
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

    def segments_for_timestamps(start_timestamp, end_timestamp, granularity = FIVE_MINUTES)
      start_ts = time_segment_for_timestamp(start_timestamp)
      end_ts = time_segment_for_timestamp(end_timestamp)

      num_ts = (end_ts - start_ts) / 1_000_000 / granularity

      segments = [start_ts]
      ts = start_ts
      while ts < end_ts
        ts += granularity
        segments << ts
      end

      raise "Error calculating time segments!" unless segments[-1] == end_ts

      segments
    end

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

        cf = subtype[:cf][options[:subtype]]   # Example: :StatsGauge
        raise "No such subtype as #{options[:subtype]}!" unless cf
        value_name = subtype[:value][options[:subtype]] # Example: :value
        # value_name may be nil, so don't raise on nil
      end

      segments = segments_for_timestamps(start_timestamp, end_timestamp, granularity)
      row_keys = segments.map { |seg| "#{client_uuid}-#{seg}" }

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
