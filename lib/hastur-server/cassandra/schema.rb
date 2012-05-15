require "cassandra"
require "msgpack"
require "multi_json"
require "date"

require "hastur-server/util"

module Hastur
  module Cassandra
    extend self

    # These constants aren't so much "public" as they are useful
    # in-module and we don't care about securing them.

    ONE_SECOND = 1_000_000
    FIVE_MINUTES = 5 * 60 * ONE_SECOND
    CASS_GET_OPTIONS = [ :consistency, :count, :start, :finish, :reversed ]
    ONE_HOUR = 12 * FIVE_MINUTES
    ONE_DAY = 24 * ONE_HOUR
    ONE_WEEK = 7 * ONE_DAY

    SCHEMA = {
      "gauge" => {
        :cf          => :GaugeArchive,
        :name        => :name,
        :value       => :value,
        :granularity => FIVE_MINUTES,
        :values_cf   => :StatGauge,
        :name_cf     => :GaugeNameDay,
      },
      "counter" => {
        :cf          => :CounterArchive,
        :name        => :name,
        :value       => :value,
        :granularity => FIVE_MINUTES,
        :values_cf   => :StatCounter,
        :name_cf     => :CounterNameDay,
      },
      "mark" => {
        :cf          => :MarkArchive,
        :name        => :name,
        :value       => :value,
        :granularity => ONE_HOUR,
        :values_cf   => :StatMark,
        :name_cf     => :MarkNameDay,
      },
      "log" => {
        :cf          => :LogArchive,
        :granularity => FIVE_MINUTES,
        :name        => nil,
      },
      "error" => {
        :cf          => :ErrorArchive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
      "event" => {
        :cf          => :EventArchive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
      "hb_process" => {
        :cf          => :HBProcessArchive,
        :name        => :name,
        :value       => :value,
        :granularity => FIVE_MINUTES,
        :values_cf   => "HBProcess",
        :name_cf     => "HBProcessNameDay",
      },
      "hb_agent" => {
        :cf          => :HBAgentArchive,
        :name        => :name,
        :value       => :value,
        :granularity => ONE_HOUR,
        :values_cf   => "HBAgent",
      },
      "hb_pluginv1" => {
        :cf          => :HBPluginV1Archive,
        :name        => :name,
        :value       => :value,
        :granularity => ONE_HOUR,
        :values_cf   => "HBPluginV1",
        :name_cf     => "HBPluginV1NameDay",
      },
      "reg_agent" => {
        :cf          => :RegAgentArchive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
      "reg_process" => {
        :cf          => :RegProcessArchive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
      "reg_pluginv1" => {
        :cf          => :RegPluginV1Archive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
      "info_process" => {
        :cf          => :InfoProcessArchive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
      "info_agent" => {
        :cf          => :InfoAgentArchive,
        :granularity => ONE_DAY,
        :name        => nil,
      },
    }.freeze

    #
    # Return a list of CassandraThrift::CfDef objects that can be used for setup.
    # @param [String] keyspace the cfdefs will be instantiated in.
    # @param [Hash]  additional options for CassandraThrift::CfDef.new
    # @return [Array<CassandraThrift::CfDef>]
    #
    def cfdefs(keyspace, opts={})
      SCHEMA.values.map do |data|
        CassandraThrift::CfDef.new *opts, :name => data[:cf].to_s, :keyspace => keyspace
      end
    end

    # Options from Twitter Cassandra gem:
    #   :ttl
    #   :consistency
    # Additional options:
    #   :uuid - agent UUID
    #   :msg_type - data type from the hastur message (required)
    def insert(cass_client, json_string, msg_type, options = {})
      hash = MultiJson.load(json_string, :symbolize_keys => true)
      raise "Cannot deserialize JSON string!" unless hash
      uuid = options.delete(:uuid) || hash[:uuid] || hash[:from]
      raise "No UUID given!" unless uuid

      schema = SCHEMA[msg_type]
      raise "No schema defined for Hastur message type '#{msg_type}'!" unless schema

      name = schema[:name] ? hash[schema[:name]] : nil
      value = hash[:value]                   # Example: 37.914
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
        cf = schema[:values_cf]
        client.insert(cf, key, { colname => value.to_msgpack, "last_write" => now_ts,
                        "last_access" => now_ts }, insert_options) if cf

        # Insert into "saw this in this time period" rows
        client.insert(:UUIDDay, one_day_ts.to_s, { uuid => "" })
        if schema[:name_cf]
          client.insert(schema[:name_cf], one_day_ts.to_s, { name => "" })
        end
      end
    end

    # Get a message on a given agent UUID over a given block of time, up to about a day.
    #
    # Options:
    #   :name - message name
    #   :consistency - Cassandra read consistency
    #   :count - maximum number of entries to return, default 10000
    #
    def get(cass_client, agent_uuid, type, start_timestamp, end_timestamp, options = {})
      if end_timestamp - start_timestamp > 72 * ONE_HOUR
        raise "Don't query more than 3 days at once yet!"
      end

      raw_get_all(cass_client, agent_uuid, type, start_timestamp, end_timestamp, options)
    end

    # Get all stats on a given agent UUID over a given block of time, up to about a day.
    # If a :type option is given, pull the values from that type's storage area.  Otherwise,
    # pull raw JSON information from the all-stats archive area.
    #
    # Options:
    #   :value_only - return only the value, not the full JSON
    #   :consistency - Cassandra read consistency
    #   :count - maximum number of entries to return, default 10000
    #
    def get_all_stats(cass_client, agent_uuid, start_timestamp, end_timestamp, options = {})
      if end_timestamp - start_timestamp > 72 * ONE_HOUR
        raise "Don't query more than 3 days at once yet!"
      end

      r1 = raw_get_all(cass_client, agent_uuid, "gauge", start_timestamp, end_timestamp, options) || {}
      r2 = raw_get_all(cass_client, agent_uuid, "counter", start_timestamp, end_timestamp, options) || {}
      r3 = raw_get_all(cass_client, agent_uuid, "mark", start_timestamp, end_timestamp, options) || {}
      r1.merge(r2).merge(r3)
    end

    protected

    def time_segment_for_timestamp(timestamp, granularity)
      # :name, :value, :timestamp
      time = Time.at(timestamp / ONE_SECOND)

      date = time.to_date

      if granularity == ONE_WEEK
        one_day_seconds = 24 * 60 * 60
        start_of_week = date - date.wday * one_day_seconds   # In sec, not usec
        return Time.utc(start_of_week.year, start_of_week.month, start_of_week.day).to_i * ONE_SECOND
      end

      # Timestamp in seconds of start of day
      date_secs = Time.utc(date.year, date.month, date.day).to_i

      # How many seconds we are into the day in UTC
      usecs_into_day = (time - date_secs).to_i * ONE_SECOND

      chunks_into_day = (usecs_into_day / granularity).to_i
      time_division = chunks_into_day * granularity

      # This is the time, rounded down to the nearest 'granularity' seconds from start of day
      date_secs * ONE_SECOND + time_division
    end

    def row_key(uuid, timestamp, granularity)
      time_segment = time_segment_for_timestamp(timestamp, granularity)

      # The row key uses the agent ID spelled out in hex, not compressed to 128 bits.
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
    # message type and agent UUID across the given timestamps.  It can be modified in several
    # other ways by options:
    #
    # Hastur Options:
    #   :name - the message name such as stat name, heartbeat name or plugin name
    #   :value_only - return only the message (usually stat) value, not full JSON
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
    def raw_get_all(cass_client, agent_uuid, msg_type, start_timestamp, end_timestamp, options = {})
      if options[:name] && (options[:start] || options[:finish])
        raise "Error: you can't specify the :name option with :start or :finish in raw_get_all!"
      end

      schema = SCHEMA[msg_type]
      raise "No schema defined for Hastur message type '#{msg_type}'!" unless schema

      name_field = schema[:name]
      granularity = schema[:granularity]

      cf = schema[:cf]
      cf = schema[:values_cf] if options[:value_only] && schema[:values_cf]

      segments = segments_for_timestamps(start_timestamp, end_timestamp, granularity)

      if agent_uuid.kind_of?(Array)
        row_keys = agent_uuid.map do |uuid|
          segments.map { |seg| "#{uuid}-#{seg}" }
        end.flatten
      else
        row_keys = segments.map { |seg| "#{agent_uuid}-#{seg}" }
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
        # For an unnamed schema like events, tell Cassandra what column range to query
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
            val = options[:value_only] ? MessagePack.unpack(value) : value
            if name
              final_values[name] ||= {}
              final_values[name][timestamp] = val
            else
              final_values[timestamp] = val
            end
          end
        end
      end

      final_values
    end
  end
end
