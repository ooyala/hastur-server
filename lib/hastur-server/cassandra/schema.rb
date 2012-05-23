require "cassandra"
require "msgpack"
require "multi_json"
require "date"

require "hastur-server/util"
require "hastur-server/message"

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
        :type        => :gauge,
        :archive_cf  => :GaugeArchive,
        :name        => true,
        :value       => :value,
        :granularity => FIVE_MINUTES,
        :values_cf   => :StatGauge,
        :name_cf     => :GaugeNameDay,
        :metadata_cf => :GaugeMetadata,
      },
      "counter" => {
        :type        => :counter,
        :archive_cf  => :CounterArchive,
        :name        => true,
        :value       => :value,
        :granularity => FIVE_MINUTES,
        :values_cf   => :StatCounter,
        :name_cf     => :CounterNameDay,
        :metadata_cf => :CounterMetadata,
      },
      "mark" => {
        :type        => :mark,
        :archive_cf  => :MarkArchive,
        :name        => true,
        :value       => :value,
        :granularity => ONE_HOUR,
        :values_cf   => :StatMark,
        :name_cf     => :MarkNameDay,
        :metadata_cf => :MarkMetadata,
      },
      "log" => {
        :type        => :log,
        :archive_cf  => :LogArchive,
        :granularity => FIVE_MINUTES,
        :name        => false,
        :metadata_cf => :LogMetadata,
      },
      "error" => {
        :type        => :error,
        :archive_cf  => :ErrorArchive,
        :granularity => ONE_DAY,
        :name        => false,
        :metadata_cf => :ErrorMetadata,
      },
      "event" => {
        :type        => :event,
        :archive_cf  => :EventArchive,
        :granularity => ONE_DAY,
        :name        => true,
        :metadata_cf => :EventMetadata,
      },
      "hb_process" => {
        :type        => :hb_process,
        :archive_cf  => :HBProcessArchive,
        :name        => true,
        :value       => :value,
        :granularity => FIVE_MINUTES,
        :values_cf   => "HBProcess",
        :name_cf     => "HBProcessNameDay",
        :metadata_cf => :HBProcessMetadata,
      },
      "hb_agent" => {
        :type        => :hb_agent,
        :archive_cf  => :HBAgentArchive,
        :name        => true,
        :value       => :value,
        :granularity => ONE_HOUR,
        :values_cf   => "HBAgent",
        :metadata_cf => :HBAgentMetadata,
      },
      "hb_pluginv1" => {
        :type        => :hb_pluginv1,
        :archive_cf  => :HBPluginV1Archive,
        :name        => true,
        :value       => :value,
        :granularity => ONE_HOUR,
        :values_cf   => "HBPluginV1",
        :name_cf     => "HBPluginV1NameDay",
        :metadata_cf => :HBPluginV1Metadata,
      },
      "reg_agent" => {
        :type        => :reg_agent,
        :archive_cf  => :RegAgentArchive,
        :granularity => ONE_DAY,
        :name        => false,
        :metadata_cf => :RegAgentMetadata,
      },
      "reg_process" => {
        :type        => :reg_process,
        :archive_cf  => :RegProcessArchive,
        :granularity => ONE_DAY,
        :name        => false,
        :metadata_cf => :RegProcessMetadata,
      },
      "reg_pluginv1" => {
        :type        => :reg_pluginv1,
        :archive_cf  => :RegPluginV1Archive,
        :granularity => ONE_DAY,
        :name        => false,
        :metadata_cf => :RegPluginV1Metadata,
      },
      "info_process" => {
        :type        => :info_process,
        :archive_cf  => :InfoProcessArchive,
        :granularity => ONE_DAY,
        :name        => false,
        :metadata_cf => :InfoProcessMetadata,
      },
      "info_agent" => {
        :type        => :info_agent,
        :archive_cf  => :InfoAgentArchive,
        :granularity => ONE_DAY,
        :name        => false,
        :metadata_cf => :InfoAgentMetadata,
      },
    }.freeze

    #
    # Insert a column.
    #
    # @param [Cassandra] cass_client client object, should be connected and in the right keyspace
    # @param [String] json_string to be parsed & data used for the insert
    # @param [Hash] schema The schema hash for this message type
    # @param [Hash{Symbol=>Fixnum,String}] options
    # @option options [Fixnum] :ttl, passed to the cassandra client
    # @option options [Fixnum] :consistency, passed to the cassandra client
    # @option options [String] :uuid 36-byte agent UUID
    #
    def insert(cass_client, json_string, msg_type, options = {})
      schema_insert(cass_client, json_string, SCHEMA[msg_type], options)
    end

    def schema_insert(cass_client, json_string, schema, options = {})
      hash = MultiJson.load(json_string, :symbolize_keys => true)
      raise "Cannot deserialize JSON string!" unless hash
      uuid = options.delete(:uuid) || hash[:uuid] || hash[:from]
      raise "No UUID given!" unless uuid

      name = schema[:name] ? hash[:name] : nil
      value = hash[:value]
      timestamp_usec = hash[:timestamp]

      colname = col_name(name, timestamp_usec)
      key = ::Hastur::Cassandra.row_key(uuid, timestamp_usec, schema[:granularity] || ONE_DAY)
      one_day_ts = time_segment_for_timestamp(timestamp_usec, ONE_DAY)

      insert_options = { }
      insert_options[:consistency] = options[:consistency] if options[:consistency]
      now_ts = ::Hastur::Util.timestamp.to_s
      cass_client.batch do |client|
        client.insert(schema[:archive_cf], key, { colname => json_string }, insert_options)
        client.insert(schema[:metadata_cf], key,
                      { "last_write" => now_ts, "last_access" => now_ts }, insert_options)

        cf = schema[:values_cf]
        client.insert(cf, key, { colname => value.to_msgpack }, insert_options) if cf

        # Insert into "saw this in this time period" rows
        client.insert(:LookupByKey, "uuid-#{one_day_ts}", { uuid => "" }, {})
        if schema[:name]
          type_id = Hastur::Message.symbol_to_type_id(schema[:type])
          client.insert(:LookupByKey, "name-#{one_day_ts}", { "#{name}-#{type_id}" => "" }, {})
        end
      end
    end

    #
    # This is the basic getter for messages.  By default it gets all
    # messages with the given message type(s) and agent UUID(s) across
    # the given timestamps.
    #
    # Queries should be broken up into no more than a day or so each
    # time, less for queries across multiple UUIDs or message types.
    #
    # You can specify :name or :name_prefix but not both at once.
    # Either one is incompatible with directly specifying Cassandra
    # :start/:finish options -- but if you're doing that, you should
    # already be directly reading Hastur code.  It's an ugly hack and
    # you will need to adjust it periodically as we change our storage
    # schema.  If possible, don't.
    #
    # @param cass_client The cassandra client object
    # @param [Array<String> or String] agent_uuids The UUID or list of UUIDs to query
    # @param [String or Symbol or Hash or Array] type The message type(s) or schema(s) to query
    # @param [Fixnum] start_timestamp The earliest time value to query
    # @param [Fixnum] end_timestamp The latest time value to query
    # @param [Hash] options Options
    # @option options [String] :name The message name
    # @option options [String] :name_prefix The message name prefix
    # @option options [Boolean] :value_only Return only message values, not full JSON
    # @option options [Fixnum] :count Maximum number to return, defaults to 10_000
    # @option options [Fixnum] :consistency Read consistency, defaults to 1
    # @option options [String] :start Initial column name for a Cassandra slice - use at own risk!
    # @option options [String] :finish Final column name for a Cassandra slice - use at own risk!
    # @option options [Boolean] :reversed Return in reverse order
    #
    def get(cass_client, agent_uuid, type, start_timestamp, end_timestamp, options = {})
      if end_timestamp - start_timestamp > 72 * ONE_HOUR
        raise "Don't query more than 3 days at once yet!"
      end

      # Make sure type is a list
      type = [type].flatten

      # If it's a list of strings/symbols, convert to schema objects
      unless type[0].is_a?(Hash)
        schemas = type.map { |type| SCHEMA[type.to_s] }
      end

      raw_get_all(cass_client, agent_uuid, schemas.compact, start_timestamp, end_timestamp, options)
    end

    #
    # Get all stats on a given agent UUID over a given block of time, up to about a day.
    # If a :type option is given, pull the values from that type's storage area.  Otherwise,
    # pull raw JSON information from the all-stats archive area.
    #
    # Options:
    #   :name
    #   :value_only - return only the value, not the full JSON
    #   :consistency - Cassandra read consistency
    #   :count - maximum number of entries to return, default 10000
    #
    def get_all_stats(cass_client, agent_uuid, start_timestamp, end_timestamp, options = {})
      if end_timestamp - start_timestamp > 72 * ONE_HOUR
        raise "Don't query more than 3 days at once yet!"
      end

      get(cass_client, agent_uuid, [ "gauge", "counter", "mark" ],
          start_timestamp, end_timestamp, options) || {}
    end

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
      timestamp = time_packed.unpack("Q>")[0].to_i

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

    protected

    #
    # Basic raw low-level getter.  See .get() for options and params,
    # but types must be converted to schemas already.
    #
    def raw_get_all(cass_client, agent_uuids, msg_schemas, start_timestamp, end_timestamp, options = {})
      if (options[:name] && options[:name_prefix]) ||
          (options[:name] || options[:name_prefix]) && (options[:start] || options[:finish])
        raise "Error: you can have at most one of :name, :name_prefix or :start/:finish in raw_get_all!"
      end

      # Make sure start_timestamp and end_timestamp are in the right order.
      if start_timestamp > end_timestamp
        tmp = end_timestamp
        end_timestamp = start_timestamp
        start_timestamp = tmp
      end

      # Coerce to list
      agent_uuids = [ agent_uuids ].flatten
      msg_schemas = [ msg_schemas ].flatten

      if options[:name] || options[:name_prefix]
        # Want a name?  Then filter out all nameless schemas.
        msg_schemas = msg_schemas.select { |schema| schema[:name] }

        if msg_schemas.empty?
          raise "You asked for messages by name, but gave only types with no name!"
          return {}
        end
      end

      cf_by_type = {}
      row_keys_by_type = {}
      metadata_row_keys_by_type = {}
      options_by_type = {}

      msg_schemas.each do |schema|
        type = schema[:type]

        cf = (options[:value_only] && schema[:values_cf]) ? schema[:values_cf] : schema[:archive_cf]
        cf_by_type[type] = cf

        row_keys_by_type[type] = agent_uuids.map do |uuid|
          segments = segments_for_timestamps(start_timestamp, end_timestamp, schema[:granularity])
          segments.map { |seg| "#{uuid}-#{seg}" }
        end.flatten

        metadata_row_keys_by_type[type] = agent_uuids.map do |uuid|
          segments = segments_for_timestamps(start_timestamp, end_timestamp, ONE_DAY)
          segments.map { |seg| "#{uuid}-#{seg}" }
        end.flatten

        cass_options = { :count => 10_000 }
        CASS_GET_OPTIONS.each do |opt|
          cass_options[opt] = options[opt] if options.has_key?(opt)
        end

        if schema[:name] && options[:name_prefix]
          prefix = options[:name_prefix]
          raise "We currently fail hard if the last byte of the name prefix is 255!" if prefix[-1].ord == 255

          # We use a reversed comparator - swap start and finish
          cass_options[:finish] = prefix
          cass_options[:start] = prefix[0..-2] + prefix[-1].succ
        elsif schema[:name] && options[:name]
          # For a named schema like stats or heartbeats, tell Cassandra what column range to query.
          # Reverse the timestamps.  We use a reverse comparator, we have to.
          cass_options[:finish] = col_name(options[:name], start_timestamp)
          cass_options[:start] = col_name(options[:name], end_timestamp)
        elsif !schema[:name]
          # For an unnamed schema like events, tell Cassandra what column range to query
          cass_options[:start] = col_name(nil, start_timestamp) unless options[:start]
          cass_options[:finish] = col_name(nil, end_timestamp) unless options[:finish]
        else
          # The schema has a name, but we're not specifying it.  Don't specify a start
          # or finish unless the caller explicitly gave one.
        end

        options_by_type[type] = cass_options
      end

      values = {}
      options_by_type.each do |type, cass_options|
        # Now, actually do the query
        values[type] = cass_client.multi_get(cf_by_type[type], row_keys_by_type[type], cass_options)
      end

      # Mark rows as accessed
      now_ts = Hastur::Util.timestamp(nil)
      cass_client.batch do |client|
        msg_schemas.each do |schema|
          row_keys = metadata_row_keys_by_type[schema[:type]]

          row_keys.each do |row_key|
            client.insert(schema[:metadata_cf], row_key, { "last_access" => now_ts.to_s }, {})
          end
        end
      end

      # Delete empty rows in result
      values.each { |_, hash| hash.delete_if { |_, value| value.nil? || value.empty? } }

      final_values = {}
      values.each do |type, v|
        v.each do |row_key, col_hash|
          uuid = uuid_from_row_key(row_key)
          final_values[uuid] ||= {}
          final_values[uuid][type.to_s] ||= {}
          hash = final_values[uuid][type.to_s]

          col_hash.each do |col_key, value|
            name, timestamp = col_name_to_name_and_timestamp(col_key)

            if timestamp <= end_timestamp && timestamp >= start_timestamp
              val = options[:value_only] ? MessagePack.unpack(value) : value

              if name
                hash[name] ||= {}
                hash[name][timestamp] = val
              else
                hash[timestamp] = val
              end
            end
          end
        end
      end

      final_values
    end
  end
end
