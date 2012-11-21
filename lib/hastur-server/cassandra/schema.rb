require "multi_json"
require "date"

require "hastur-server/compat_msgpack"
require "hastur-server/util"
require "hastur-server/time_util"
require "hastur-server/message"

module Hastur
  module Cassandra
    include Hastur::TimeUtil
    extend self

    # These constants aren't so much "public" as they are useful
    # in-module and we don't care about securing them.

    # These need to match the Cassandra gem ConsistencyLevel constants
    CONSISTENCY_ONE = 1
    CONSISTENCY_QUORUM = 2
    CONSISTENCY_LOCAL_QUORUM = 3
    CONSISTENCY_EACH_QUORUM = 4
    CONSISTENCY_ALL = 5
    CONSISTENCY_ANY = 6
    CONSISTENCY_TWO = 7
    CONSISTENCY_THREE = 8

    ONE_SECOND = 1_000_000
    FIVE_MINUTES = 5 * 60 * ONE_SECOND
    CASS_GET_OPTIONS = [ :consistency, :count, :start, :finish, :reversed ]
    ONE_HOUR = 12 * FIVE_MINUTES
    ONE_DAY = 24 * ONE_HOUR
    ONE_WEEK = 7 * ONE_DAY
    DEFAULT_WRITE_CONSISTENCY = CONSISTENCY_TWO
    DEFAULT_READ_CONSISTENCY = CONSISTENCY_TWO

    # A Hastur Schema is a mapping of strings to symbols to values.
    # The top level strings are the type names, the symbols are
    # attributes of those types (:type, :archive_cf, :granularity,
    # :name, :name_cf, :value, :values_cf, :rollup_cf, :metadata_cf).

    def current_schemas
      # Frozen hash or nil
      @hastur_schemas
    end

    def schema_by_type(type)
      unless @already_loaded_schema_file
        location = ENV['HASTUR_SCHEMA_FILE'] ||
          File.join(File.dirname(__FILE__), "..", "..", "..", "tools", "hastur_schema.json")

        # Read the JSON objects.
        # Symbolize both keys and string values of the individual schemas.

        begin
          contents = File.read location
          hash = MultiJson.load(contents)
          hash.each do |type, type_hash|
            type_hash.keys.each do |key|
              value = type_hash[key]
              if value.is_a?(String)
                type_hash[key.to_sym] = value.to_sym
              else
                type_hash[key.to_sym] = value
              end
              type_hash.delete(key)
            end
          end
        rescue
          raise "Failed to read schema from #{location}, failed with exception #{$!.inspect}!"
        end

        @hastur_schemas = hash.freeze

        @already_loaded_schema_file = true
      end

      @hastur_schemas[type.to_s]
    end

    #
    # Insert a Hastur message.  This inserts the JSON into the archive
    # column family, the value into the value column family (if
    # relevant), and updates the metadata and lookup_by_key column
    # family.
    #
    # @param [Cassandra] cass_client client object, should be connected and in the right keyspace
    # @param [String] json_string to be parsed & data used for the insert
    # @param [Hash] schema The schema hash for this message type
    # @param [Hash{Symbol=>Fixnum,String}] options
    # @option options [Fixnum] :ttl, in seconds, passed to the cassandra client
    # @option options [Fixnum] :consistency, passed to the cassandra client
    # @option options [String] :uuid 36-byte agent UUID
    #
    def insert(cass_client, json_string, schema, options = {})
      unless schema.is_a?(Hash)
        schema = schema_by_type(schema)
      end

      hash = MultiJson.load(json_string)
      raise "Cannot deserialize JSON string!" unless hash
      uuid = options.delete(:uuid) || hash[:uuid] || hash[:from]
      raise "No UUID given!" unless uuid

      hash["labels"] ||= {}

      name = schema[:name] ? hash["name"] : nil
      value = hash["value"]
      timestamp_usec = hash["timestamp"]
      app_name = hash["labels"]["app"] || ""

      colname = col_name(name, timestamp_usec)
      key = ::Hastur::Cassandra.row_key(uuid, timestamp_usec, schema[:granularity] || ONE_DAY)
      one_day_ts = time_segment_for_timestamp(timestamp_usec, ONE_DAY)

      insert_options = { :consistency => options[:consistency] || DEFAULT_WRITE_CONSISTENCY }
      insert_options[:ttl] = options[:ttl] if options[:ttl]
      now_ts = ::Hastur::Util.timestamp.to_s

      cass_client.batch do |client|
        client.insert(schema[:archive_cf], key, { colname => json_string }, insert_options)

        client.insert(schema[:metadata_cf], key,
                      { "last_write" => now_ts, "last_access" => now_ts }, insert_options)

        cf = schema[:values_cf]
        client.insert(cf, key, { colname => value.to_msgpack }, insert_options) if cf

        # Insert into "saw UUID in this time period" row
        client.insert(:lookup_by_key, "uuid-#{one_day_ts}", { uuid => "" }, insert_options)

        # Insert into "saw message name in this time period" row
        if schema[:name]
          type_id = Hastur::Message.symbol_to_type_id(schema[:type])
          colkey = [name, type_id, uuid].join('-')
          client.insert(:lookup_by_key, "name-#{one_day_ts}", { colkey => "" }, insert_options)
        end

        # Insert into "saw this UUID for this app name" row
        client.insert(:lookup_by_key, "app_name-#{one_day_ts}", { "#{app_name}-#{uuid}" => "" }, insert_options)
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
      if end_timestamp - start_timestamp > 32 * ONE_DAY
        raise "Querying more than a month at a time is unsupported."
      end

      # Make sure type is a list
      type = [type].flatten

      # If it's a list of strings/symbols, convert to schema objects
      unless type[0].is_a?(Hash)
        schemas = type.map { |type| schema_by_type(type) }
      end

      raw_get_all(cass_client, agent_uuid, schemas.compact, start_timestamp, end_timestamp, options)
    end

    #
    # Get one or more rows from the lookup_by_key CF and return a flattened hash.
    #
    # @param cass_client The cassandra client object
    # @param [String,Symbol] prefix the row key prefix to fetch, e.g. "name", "cnames"
    # @param [Fixnum] start_timestamp The earliest time value to query
    # @param [Fixnum] end_timestamp The latest time value to query
    #
    # @example
    #   names = Hastur::Cassandra.lookup_by_key(client, "name", Time.now - 86401, Time.now)
    #
    def lookup_by_key(cass_client, kind, start_timestamp, end_timestamp, options={})
      data = Hash.new
      options = { :count => 10_000 }.merge(options)
      # this isn't defined in the schema (yet?) so the bucket is hard-coded to one day everywhere
      usec_aligned_chunks(start_timestamp, end_timestamp, :day).each do |ts|
        cass_client.get('lookup_by_key', "#{kind}-#{ts}", options).each do |key,value|
          data[key] = value
        end
      end
      data
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

    #
    # Generate a list of row keys from a uuid, bucket size, and time range.
    #
    # @param [Array<String>] uuids array of 36-byte uuids
    # @param [Symbol,Fixnum] bucket_size row's bucket size either symbol or number of usecs
    # @param [Fixnum] start_timestamp start of the range
    # @param [Fixnum] end_timestamp end of the range
    # @return [Array<String>] array of row keys
    #
    def row_keys(uuids, bucket_size, start_timestamp, end_timestamp, options={})
      times = usec_aligned_chunks(start_timestamp, end_timestamp, bucket_size)
      out = []
      times.each do |ts|
        uuids.each do |uuid|
          if options[:rollup_period]
            out << "#{uuid}-#{options[:rollup_period]}-#{ts}"
          end

          unless options[:rollup_only]
            out << "#{uuid}-#{ts}"
          end
        end
      end
      out
    end

    def col_name(name, timestamp)
      if name
        colname = "#{name.force_encoding("UTF-8")}-#{[timestamp].pack("Q>")}"
      else
        colname = [timestamp].pack("Q>")
      end
    end

    def uuid_from_row_key(row_key)
      row_key.split("-")[0..4].join("-")
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
    # TODO(al) this method is too big and must be broken up
    #
    def raw_get_all(cass_client, agent_uuids, msg_schemas, start_ts, end_ts, options = {})
      if (options[:name] && options[:name_prefix]) ||
          (options[:name] || options[:name_prefix]) && (options[:start] || options[:finish])
        raise "Error: you can have at most one of :name, :name_prefix or :start/:finish in raw_get_all!"
      end

      # Make sure start_timestamp and end_timestamp are in the right order.
      if start_ts > end_ts
        tmp = end_ts
        end_ts = start_ts
        start_ts = tmp
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
        granularity = schema[:granularity]
        meta_granularity = USEC_ONE_DAY

        if options[:value_only] and schema[:values_cf]
          cf = schema[:values_cf]
        elsif options[:rollup_period] and schema[:rollup_cf]
          cf = schema[:rollup_cf]
          granularity = meta_granularity = USEC_ONE_WEEK
        else
          cf = schema[:archive_cf]
        end

        type = schema[:type]
        cf_by_type[type] = cf

        row_keys_by_type[type] = row_keys(agent_uuids, granularity, start_ts, end_ts, options)
        metadata_row_keys_by_type[type] = row_keys(agent_uuids, meta_granularity, start_ts, end_ts, options)

        cass_options = { :count => 10_000, :consistency => DEFAULT_READ_CONSISTENCY }
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
          cass_options[:finish] = col_name(options[:name], start_ts)
          cass_options[:start] = col_name(options[:name], end_ts)
        elsif !schema[:name]
          # For an unnamed schema like events, tell Cassandra what column range to query
          cass_options[:finish] = col_name(nil, start_ts) unless options[:finish]
          cass_options[:start] = col_name(nil, end_ts) unless options[:start]
        else
          # The schema has a name, but we're not specifying it.  Don't specify a start
          # or finish unless the caller explicitly gave one.
        end

        options_by_type[type] = cass_options
      end

      values = {}
      options_by_type.each do |type, cass_options|
        # Now, actually do the query
        begin
          if options[:count_columns]
            values[type] = cass_client.multi_count_columns(cf_by_type[type], row_keys_by_type[type], cass_options)
          else
            values[type] = cass_client.multi_get(cf_by_type[type], row_keys_by_type[type], cass_options)
          end
        # the Cassandra gem tends to return this useless and misleading exception,
        # so catch it and raise something with some useful info in it
        #rescue ThriftClient::NoServersAvailable
        #  raise "query failed: type: #{type} column_family: #{cf_by_type[type]}, row_keys: #{row_keys_by_type[type]}, cass_options: #{cass_options}, options: #{options}, reason: #{$!.message}"
        end
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

      if options[:count_columns]
        #TODO(noah): Fix this
      end

      # Delete empty rows in result
      values.each { |_, hash| hash.delete_if { |_, value| value.nil? || value.empty? } }

      # Final output format:  { :uuid => { :type => { :name => { :timestamp => value } } } }
      final_values = {}
      values.each do |type, v|
        v.each do |row_key, col_hash|
          uuid = uuid_from_row_key(row_key)
          final_values[uuid] ||= {}
          final_values[uuid][type.to_s] ||= {}
          hash = final_values[uuid][type.to_s]

          col_hash.each do |col_key, value|
            name, timestamp = col_name_to_name_and_timestamp(col_key)

            if timestamp <= end_ts && timestamp >= start_ts
              hash[name] ||= {}

              # This happens even if name is nil
              if options[:value_only] or options[:rollup_period] or options[:rollup_only]
                hash[name][timestamp] = MessagePack.unpack(value) rescue value
              else
                hash[name][timestamp] = value
              end

            end
          end
        end
      end

      final_values
    end
  end
end
