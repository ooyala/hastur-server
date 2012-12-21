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

    SECONDS_PER_DAY = 86400

    DEFAULT_QUERY_SIZE = 20

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
    # @param [Hash] schema The schema hash for this message type, or name of type
    # @param [Hash{Symbol=>Fixnum,String}] options
    # @option options [Fixnum] :ttl_seconds TTL in seconds, passed to the cassandra client
    # @option options [Fixnum] :consistency Consistency, passed to the cassandra client
    # @option options [String] :uuid 36-byte agent UUID if not present in JSON data
    # @option options [Fixnum] :request_ts Timestamp for columns, defaults to now
    #
    def insert(cass_client, json_string, schema, options = {})
      hash = MultiJson.load(json_string)
      raise "Cannot deserialize JSON string!" unless hash
      uuid = hash["uuid"] || hash["from"] || options[:uuid]
      raise "No UUID given!" unless uuid
      ttl = options[:ttl_seconds] ? options[:ttl_seconds].to_i : (hash["ttl"] ? hash["ttl"].to_i : nil)

      if schema.nil?
        schema = schema_by_type(hash["type"].to_sym)
      elsif !schema.is_a?(Hash)
        if hash["type"] && hash["type"].to_s != schema.to_s
          raise "Types don't match! #{hash["type"].inspect} / #{schema.inspect}"
        end
        schema = schema_by_type(schema)
      end

      name = schema[:name] ? hash["name"] : nil
      type = schema[:type] || hash["type"]

      hash["labels"] ||= {}

      colname = col_name(name, hash["timestamp"])
      key = ::Hastur::Cassandra.row_key(uuid, hash["timestamp"], schema[:granularity] || ONE_DAY)
      one_day_ts = time_segment_for_timestamp(hash["timestamp"], ONE_DAY)

      insert_options = { :consistency => options[:consistency] || DEFAULT_WRITE_CONSISTENCY }
      insert_options[:ttl_seconds] = options[:ttl_seconds] if options[:ttl_seconds]
      now_ts = (options[:request_ts] || ::Hastur::Util.timestamp).to_s

      indexes = indexes_for_message(hash, schema, options)

      cass_client.batch do |client|
        ttl = nil
        indexes.each do |idx_cf, row_hash|
          ttl = row_hash[:ttl_seconds]  # Use TTL if set
          row_hash.each do |idx_row_key, col_hash|
            next if idx_row_key == :ttl_seconds

            cass_options = insert_options
            cass_options = cass_options.merge(:ttl_seconds => ttl) if ttl

            col_hash.each do |idx_col_key, idx_col_val|
              client.insert(idx_cf, idx_row_key, { idx_col_key => idx_col_val }, cass_options)
            end
          end
        end

        client.insert(schema[:archive_cf], key, { colname => json_string }, insert_options)

        cf = schema[:values_cf]
        client.insert(cf, key, { colname => hash["value"].to_msgpack }, insert_options) if cf
      end
    end

    #
    # Calculated all indices to be inserted for a given message.
    # This includes things like the UUID and app name lookup,
    # but doesn't include the base value/archive items or
    # the last written and accessed metadata.
    #
    # Also, yes, the correct word is "indices".  But how many
    # people grep for that?
    #
    def indexes_for_message(hash, schema, options)
      uuid = hash["uuid"] || hash["from"] || options[:uuid]
      raise "No UUID given!" unless uuid
      ttl = (options[:ttl_seconds].to_i || hash["ttl"].to_i) rescue nil

      app_name = hash["labels"]["app"] || ""
      one_day_ts = time_segment_for_timestamp(hash["timestamp"], ONE_DAY)
      one_hour_ts = time_segment_for_timestamp(hash["timestamp"], ONE_HOUR)
      name = nil
      type_id = Hastur::Message.symbol_to_type_id(schema[:type])

      # Stat-name => [type, UUID] index
      name_indexes = { "lookup_by_key" => {} }
      if schema[:name]
        name = hash["name"]
        colkey = [name, type_id, uuid].join('-')
        name_indexes = { "lookup_by_key" => { "name-#{one_day_ts}" => { colkey => "" } } }
      end

      # Initialize label indexes with a 7-day TTL.  Cass TTLs are in seconds.
      label_indexes = {
        "lookup_by_label" => { :ttl_seconds => SECONDS_PER_DAY * 7 },
        "#{schema[:type]}_label_index" => { :ttl_seconds => SECONDS_PER_DAY * 7 }
      }
      hash["labels"].each do |lname, lvalue|
        label_indexes["lookup_by_label"]["uuid-#{one_hour_ts}"] ||= {}
        label_indexes["lookup_by_label"]["statname-#{uuid}-#{one_hour_ts}"] ||= {}

        label_indexes["lookup_by_label"]["uuid-#{one_hour_ts}"]["#{lname}\0#{lvalue}\0#{uuid}"] = ""
        colname = "#{lname}\0#{lvalue}\0#{type_id}\0#{name}"
        label_indexes["lookup_by_label"]["statname-#{uuid}-#{one_hour_ts}"][colname] = ""

        label_indexes["#{schema[:type]}_label_index"]["#{uuid}-#{one_hour_ts}"] ||= {}
        packed_ts = [hash["timestamp"].to_i].pack("Q>")
        colname = "#{lname}\0#{lvalue}\0#{name}\0#{packed_ts}"
        label_indexes["#{schema[:type]}_label_index"]["#{uuid}-#{one_hour_ts}"][colname] = ""
      end

      return {
        "lookup_by_key" => {
          # UUID index
          "uuid-#{one_day_ts}" => { uuid => "" },
          # App-name => UUID index
          "app_name-#{one_day_ts}" => { "#{app_name}-#{uuid}" => "" },
        }.merge(name_indexes["lookup_by_key"])
      }.merge(label_indexes)
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
    # @option options [Fixnum] :cass_query_size Maximum number of rows to multi_get from C* at once
    # @option options [Fixnum] :consistency Read consistency, defaults to 1
    # @option options [String] :start Initial column name for a Cassandra slice - use at own risk!
    # @option options [String] :finish Final column name for a Cassandra slice - use at own risk!
    # @option options [Boolean] :reversed Return in reverse order
    # @option options [Boolean] :profiler Return profiling data with query
    # @option options [Fixnum] :request_ts Timestamp for request access time and statistics, defaults to now
    #
    def get(cass_client, agent_uuid, type, start_ts, end_ts, options = {})
      if end_ts - start_ts > 32 * ONE_DAY
        raise "Querying more than a month at a time is unsupported."
      end

      # Make sure type is a list
      type = [type].flatten

      # If it's a list of strings/symbols, convert to schema objects
      unless type[0].is_a?(Hash)
        schemas = type.map { |type| schema_by_type(type) }
      end

      values, stats = raw_query_cassandra(cass_client, agent_uuid, schemas.compact,
                                          start_ts, end_ts, options)
      convert_raw_to_hastur_series(values, stats, start_ts, end_ts, options)
    end

    # This is a fast, no-frills Hastur message dumper.
    #
    # You can specify :name or :name_prefix but not both at once.
    # Either one is incompatible with directly specifying Cassandra
    # :start/:finish options.
    #
    # @param cass_client The cassandra client object
    # @param [Array<String>] agent_uuids The UUID(s) to query
    # @param [Array<String>] type The message type(s) to query
    # @param [Fixnum] start_timestamp The earliest time value to query
    # @param [Fixnum] end_timestamp The latest time value to query
    # @param [Hash] options Options
    # @option options [String] :name The message name
    # @option options [String] :name_prefix The message name prefix
    # @option options [Boolean] :value_only Return only message values, not full JSON
    # @option options [Fixnum] :count Maximum number to return, defaults to 10_000
    # @option options [Fixnum] :cass_query_size Maximum number of rows to multi_get from C* at once
    # @option options [Fixnum] :consistency Read consistency, defaults to 1
    # @option options [String] :start Initial column name for a Cassandra slice - use at own risk!
    # @option options [String] :finish Final column name for a Cassandra slice - use at own risk!
    # @option options [Boolean] :reversed Return in reverse order
    # @option options [Boolean] :profiler Return profiling data with query
    # @option options [Fixnum] :request_ts Timestamp for request access time and stats, defaults to now
    #
    def dump(cass_client, agent_uuid, type, start_timestamp, end_timestamp, options = {})
      now_ts = options[:request_ts] || Hastur::Util.timestamp(nil)

      # If it's a list of strings/symbols, convert to schema objects
      unless type[0].is_a?(Hash)
        schemas = type.map { |type| schema_by_type(type) }
      end

      opts = options.merge(:raw_astyanax => true)
      v,s = raw_query_cassandra(cass_client, agent_uuid, schemas.compact, start_timestamp, end_timestamp, opts)
      out = v.values.inject([], &:concat)
      if options[:value_only]
        out = out.map { |r, c, v| [ r, c, MessagePack.unpack(v)] }
      end

      apply_profiler_data(s, nil, now_ts)

      out
    end

    #
    # Get one or more rows from the lookup_by_key CF and return a flattened hash.
    #
    # For more options, see #get().
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
      data = {}
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
    # Look up UUIDs from the lookup_by_label CF and return a hash
    #
    # For more options, see #get().
    #
    # @param cass_client The cassandra client object
    # @param [Hash] labels the labels to look up, given as a hash of names to value prefixes.
    # @param [Fixnum] start_timestamp The earliest time value to query
    # @param [Fixnum] end_timestamp The latest time value to query
    # @return Hash A hash of the form { "labelname" => { "labelvalue" => [ uuid, uuid, uuid... ] } }
    #
    def lookup_label_uuids(cass_client, labels, start_timestamp, end_timestamp, options={})
      data = {}
      options = { :count => 10_000 }.merge(options)

      labels.each do |lname, lvalue|
        data[lname] ||= {}

        # We use a reversed comparator - swap start and finish
        prefix_start, prefix_end = prefixes_from_values([lname, lvalue])
        options[:finish] = prefix_start
        options[:start] = prefix_end

        usec_aligned_chunks(start_timestamp, end_timestamp, :hour).each do |ts|
          # Col name schema: lname\0lvalue\0uuid

          cass_client.get('lookup_by_label', "uuid-#{ts}", options).each do |col_key,_|
            data_lname, data_lvalue, uuid = col_key.split("\0")

            data[lname][data_lvalue] ||= []
            data[lname][data_lvalue] |= [ uuid ]  # Single-bar for "union"
          end
        end
      end

      data
    end

    #
    # Look up stat names and types from UUIDS and the lookup_by_label CF and return a hash
    #
    # Note that stat names can be nil and that's perfectly valid, especially for message
    # types without names.
    #
    # For more options, see #get().
    #
    # @param cass_client The cassandra client object
    # @param [Array] uuids A list of UUIDs
    # @param [Hash] labels the labels to look up, given as a hash of names to value prefixes.
    # @param [Fixnum] start_timestamp The earliest time value to query
    # @param [Fixnum] end_timestamp The latest time value to query
    # @return Hash A hash of output: lname => lvalue => type => msg_name => Array(uuids)
    #
    def lookup_label_stat_names(cass_client, uuids, labels, start_timestamp, end_timestamp, options={})
      data = {}
      options = { :count => 10_000 }.merge(options)

      time_buckets = usec_aligned_chunks(start_timestamp, end_timestamp, :hour)

      labels.each do |lname, lvalue|
        data[lname] ||= {}

        # We use a reversed comparator - swap start and finish
        prefix_start, prefix_end = prefixes_from_values([lname, lvalue])
        options[:finish] = prefix_start
        options[:start] = prefix_end

        query_rows = time_buckets.flat_map { |ts| uuids.map { |u| "statname-#{u}-#{ts}"} }

        cass_client.multi_get('lookup_by_label', query_rows, options).each do |row_key, col_hash|
          uuid = row_key[9..44]  # 36 characters, following "statname-"

          col_hash.each do |col_key, _|
            data_lname, data_lvalue, type_id, stat_name = col_key.split("\0")
            data[lname][data_lvalue] ||= {}
            label_output = data[lname][data_lvalue]

            type_str = Hastur::Message.type_id_to_symbol(type_id.to_i).to_s

            label_output[type_str] ||= {}
            label_output[type_str][stat_name] ||= []
            label_output[type_str][stat_name] |= [uuid]   # Single-bar for union
          end
        end
      end

      data
    end

    #
    # Look up timestamps for messages matching a label, using a hash formatted like output
    # from lookup_label_stat_names.  Return a hash formatted for trivial Cassandra
    # multiget of the results.
    #
    # The lookup_data is of the form: lname => lvalue => type => msg_name => Array(uuids)
    #
    # This method also removes all messages matching labels in remove_labels.
    #
    # @param cass_client The cassandra client object
    # @param [Array] uuids A list of UUIDs
    # @param [Hash] lookup_data Output data from lookup_label_stat_names
    # @param [Array] remove_labels The list of labels to remove data for
    # @param [Fixnum] start_timestamp The earliest time value to query
    # @param [Fixnum] end_timestamp The latest time value to query
    # @return Hash A hash of output: type -> row_key -> Array(col_keys)
    #
    def lookup_label_timestamps(cass_client, lookup_data, remove_labels, start_ts, end_ts, options = {})
      output = {}

      time_buckets = usec_aligned_chunks(start_ts, end_ts, :hour)

      # Reorder the lookup_data keys so that all non-removed labels are before all removed labels.
      # Otherwise we can "remove" an absent key and then have it added later.
      lookup_data_keys = lookup_data.keys
      lookup_data_keys = (lookup_data_keys - remove_labels) + (lookup_data_keys & remove_labels)

      lookup_data_keys.each do |lname|
        removing = remove_labels.include? lname

        lookup_data[lname].each do |lvalue, type_data|
          type_data.each do |type, name_hash|
            schema = schema_by_type(type)
            lookup_cf = "#{type}_label_index"
            schema[:granularity]

            output[type] = {}

            name_hash.each do |msg_name, uuids|
              query_rows = time_buckets.flat_map { |ts| uuids.map { |uuid| "#{uuid}-#{ts}" } }

              # We use a reversed comparator - swap start and end
              prefix_start, prefix_end = prefixes_from_values([lname, lvalue, msg_name || ""])
              cass_options = options.merge(:start => prefix_end, :finish => prefix_start)

              cass_client.multi_get(lookup_cf, query_rows, cass_options).each do |row_key, col_hash|
                uuid = row_key[0..35]  # 36 characters
                row_ts = row_key[37..-1].to_i
                out_ts = time_segment_for_timestamp(row_ts, schema[:granularity])
                output_row_key = "#{uuid}-#{out_ts}"
                output[type][output_row_key] ||= []

                col_hash.each do |col_key, _|
                  lname, lvalue, name, packed_ts = col_key.split("\0", 4)
                  unpacked_ts = packed_ts.unpack("Q>")[0]

                  next unless unpacked_ts <= end_ts && unpacked_ts >= start_ts

                  col_key = col_name(name, unpacked_ts)
                  if removing
                    output[type].delete output_row_key
                  else
                    output[type][output_row_key].push col_key
                  end
                end
              end
            end
          end
        end
      end

      output.keys.each do |type|
        output[type].keys.each do |row_key|
          output[type].delete(row_key) if output[type][row_key].empty?
        end

        output.delete(type) if output[type].empty?
      end

      output
    end

    #
    # Get all stats on a given agent UUID over a given block of time, up to about a day.
    # If a :type option is given, pull the values from that type's storage area.  Otherwise,
    # pull raw JSON information from the all-stats archive area.
    #
    # For options, see #get()
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
      packed = [timestamp].pack("Q>")
      if name
        colname = "#{name.force_encoding("ASCII-8BIT")}-#{packed}"
      else
        colname = packed
      end
    end

    def uuid_from_row_key(row_key)
      row_key.split("-")[0..4].join("-")
    end

    def col_name_to_name_and_timestamp(col_name)
      timestamp = col_name[-8..-1].unpack("Q>")[0].to_i
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
    # Calculated a start/end pair of Cassandra column prefixes
    # from an array of values, a wildcard character and a separator.
    #
    # These are used for start/finish values to pass to a Cassandra
    # get or multiget operation.
    #
    # A final value of nil will end with the penultimate value
    # (think label name and label value).
    #
    # @param [Array] values The values to concatenate, in order
    # @param [String] separator The separator for concatenating values
    # @param [String] wildcard The wildcard character for final prefix values
    #
    def prefixes_from_values(values, separator = "\0", wildcard = "*")
      last_value = values[-1]
      non_final = values.size > 1 ? (values[0..-2].join(separator) + separator) : ""
      prefix = "#{non_final}#{last_value.split(wildcard,2)[0]}"

      raise "We currently fail hard if the last byte of the label value prefix " +
        "is 255!" if prefix[-1].ord == 255
      prefix += "\0" if last_value && !last_value["*"]
      prefix_end = prefix[0..-2] + prefix[-1].succ

      [prefix, prefix_end]
    end

    protected

    #
    # Basic raw low-level getter.  See .get() for options and params,
    # but types must be converted to schemas already.
    #
    # TODO(noah) Rename this method to reflect its functionality better
    # when contrasted against all these lower-level query methods.  Also,
    # break it up more.
    #
    def raw_query_cassandra(cass_client, agent_uuids, msg_schemas, start_ts, end_ts, options)
      now_ts = options[:request_ts] || Hastur::Util.timestamp(nil)
      stats = {}

      if (options[:name] && options[:name_prefix]) ||
          (options[:name] || options[:name_prefix]) && (options[:start] || options[:finish])
        raise "Error: you can have at most one of :name, :name_prefix or :start/:finish " +
          "when querying Cassandra!"
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
      stats[:queried_row_count] = 0
      slice_size = options[:cass_query_size] || DEFAULT_QUERY_SIZE
      options_by_type.each do |type, cass_options|
        row_count = row_keys_by_type[type].count
        stats[:queried_row_count] += row_count

        # Now, actually do the query
        begin
          if options[:count_columns]
            #TODO(noah): Fix this
            raise "Unimplemented!"
          # if there are a lot of rows to access, fall back to getting one row at a time to reduce pressure on
          # cassandra at the cost of more roundtrips
          else
            values[type] = options[:raw_astyanax] ? [] : {}
            i = 0

            row_keys_by_type[type].each_slice(slice_size) do |slice|
              puts "Getting rows: #{i}/#{row_count} #{cf_by_type[type]} slice: #{slice_size}"

              if options[:raw_astyanax]
                values[type].push cass_client.raw_multi_get(cf_by_type[type], slice, cass_options)
              else
                values[type].merge! cass_client.multi_get(cf_by_type[type], slice, cass_options)
              end

              i += slice_size
            end

            values[type] = values[type].inject([], &:concat) if options[:raw_astyanax]
          end
        end
      end

      [values, stats]
    end

    public

    #
    # Query a given type -- get from a hash of row keys to lists of column keys.
    #
    # Return a raw astyanax array of [row / col_key / col_value] objects.
    #
    # @param cass_client Cassandra client object
    # @param [schema or Array] type The Hastur message type
    # @param [String or Symbol] kind The desired query result, usually "message" or "value"
    # @param [Hash] data_hash A mapping of row keys to column keys
    # @param [Hash] options Cassandra options
    #
    def query_cassandra_by_type_rows_cols(cass_client, type, kind, data_hash, options)
      cf_key = nil
      cf_key = :archive_cf if kind == "message"
      cf_key = :values_cf if kind == "value"
      raise "Unsupported label query of type #{kind.inspect}!" unless cf_key

      schema = schema_by_type type
      cass_client.raw_row_col_get(schema[cf_key], data_hash, options)
    end

    protected

    #
    # Converts raw data from raw_query_cassandra to Hastur output format.
    #
    def convert_raw_to_hastur_series(values, stats, start_ts, end_ts, options = {})
      if options[:count_columns]
        #TODO(noah): Fix this
        raise "Unimplemented!"
      end

      # Delete empty rows in result
      values.each { |_, hash| hash.delete_if { |_, value| value.nil? || value.empty? } }

      # Final output format:  { :uuid => { :type => { :name => { :timestamp => value } } } }
      stats[:col_count] = 0
      stats[:row_count] = 0
      final_values = {}
      values.each do |type, v|
        v.each do |row_key, col_hash|
          stats[:row_count] += 1
          uuid = uuid_from_row_key(row_key)
          final_values[uuid] ||= {}
          final_values[uuid][type.to_s] ||= {}
          hash = final_values[uuid][type.to_s]

          col_hash.each do |col_key, value|
            stats[:col_count] += 1
            name, timestamp = col_name_to_name_and_timestamp(col_key)

            if timestamp <= end_ts && timestamp >= start_ts
              hash[name] ||= {}

              # This happens even if name is nil
              # TODO(noah): What happens if you ask for messages with rollups?
              if options[:value_only] or options[:rollup_period] or options[:rollup_only]
                hash[name][timestamp] = MessagePack.unpack(value) rescue value
              else
                hash[name][timestamp] = value
              end
            end
          end
        end
      end

      now_ts = options[:request_ts] || Hastur::Util.timestamp(nil)
      stats[:query_time] = usec_epoch - now_ts

      apply_profiler_data(stats, options[:profiler] ? final_values : nil, now_ts)

      final_values
    end

    #
    # Converts data from [row_key, col_key, value] format to Hastur output format.
    #
    # TODO: convert all cass queries to use this and remove convert_raw_to_hastur_series.
    #
    def convert_list_to_hastur_series(values, stats, start_ts, end_ts, options = {})
      # Final output format:  { :uuid => { :type => { :name => { :timestamp => value } } } }
      stats[:row_count] = 0
      stats[:col_count] = 0
      final_values = {}
      last_row_key = nil
      values.each do |row_key, col_key, value|
        if(row_key != last_row_key)
          last_row_key = row_key
          stats[:row_count] += 1
          uuid = uuid_from_row_key(row_key)
          final_values[uuid] ||= {}
          final_values[uuid][type.to_s] ||= {}
          hash = final_values[uuid][type.to_s]
        end

        col_hash.each do |col_key, value|
          stats[:col_count] += 1
          name, timestamp = col_name_to_name_and_timestamp(col_key)

          if timestamp <= end_ts && timestamp >= start_ts
            hash[name] ||= {}

            # This happens even if name is nil
            # TODO(noah): What happens if you ask for messages with rollups?
            if options[:value_only] or options[:rollup_period] or options[:rollup_only]
              hash[name][timestamp] = MessagePack.unpack(value) rescue value
            else
              hash[name][timestamp] = value
            end
          end
        end
      end

      now_ts = options[:request_ts] || Hastur::Util.timestamp(nil)
      stats[:query_time] = usec_epoch - now_ts

      apply_profiler_data(stats, options[:profiler] ? final_values : nil, now_ts)

      final_values
    end

    def apply_profiler_data(stats, output, now_ts)
      stats.each do |key, val|
        stat_name = "hastur.cassandra.schema.query.#{key}"
        Hastur.gauge stat_name, val, now_ts
        if output
          output["profiler"] ||= {}
          output["profiler"]["gauge"] ||= {}
          output["profiler"]["gauge"][stat_name] = { now_ts => val }
        end
      end
    end
  end
end
