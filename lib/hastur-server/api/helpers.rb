require "logger"
require "cgi"
require "hastur/api"
require "hastur-server/api/constants"
require "hastur-server/time_util"
require "hastur-server/cassandra/schema"
require "hastur-server/cassandra/rollup"
require "hastur-server/util"
require "hastur-server/aggregation"
require "multi_json"
require "csv"

module Hastur
  module API
    module Helpers
      include Hastur::TimeUtil # import all the usec_* methods
      include Hastur::API::Constants
      extend self

      #
      # Get the time range tuple.
      #
      # @param [Hash{String => String}] params
      # @param [Symbol,String,Fixnum] default delta from current time for start_ts
      # @return Array<Fixnum> start and end epoch usec values
      #
      def get_start_end(default_delta = :five_minutes)
        now = env[:hastur_timestamp] || Hastur.timestamp

        if params[:end]
          end_ts = Hastur.timestamp(params[:end].to_i)
        else
          end_ts = now
        end

        if params[:start] && params[:start].to_i != 0
          start_ts = Hastur.timestamp(params[:start].to_i)
        elsif params[:ago]
          ago_usecs = usec_from_interval(params[:ago])
          start_ts = now - ago_usecs
        else
          start_ts = end_ts - usec_from_interval(default_delta)
        end

        return start_ts, end_ts
      end

      def type_list_from_string(types)
        (types || "all").split(",").map { |type| TYPES[type.to_sym] || type }.flatten.uniq
      end

      def param_is_true(value)
        value && !["", "0", "false", "no", "f"].include?(value.downcase)
      end

      CONSISTENCY_PARAMS = {
        "1" => ::Hastur::Cassandra::CONSISTENCY_ONE,
        "one" => ::Hastur::Cassandra::CONSISTENCY_ONE,
        "2" => ::Hastur::Cassandra::CONSISTENCY_TWO,
        "two" => ::Hastur::Cassandra::CONSISTENCY_TWO,
        "3" => ::Hastur::Cassandra::CONSISTENCY_THREE,
        "three" => ::Hastur::Cassandra::CONSISTENCY_THREE,
        "q" => ::Hastur::Cassandra::CONSISTENCY_QUORUM,
        "quorum" => ::Hastur::Cassandra::CONSISTENCY_QUORUM,
        "lq" => ::Hastur::Cassandra::CONSISTENCY_LOCAL_QUORUM,
        "local_quorum" => ::Hastur::Cassandra::CONSISTENCY_LOCAL_QUORUM,
        "local" => ::Hastur::Cassandra::CONSISTENCY_LOCAL_QUORUM,
        "local quorum" => ::Hastur::Cassandra::CONSISTENCY_LOCAL_QUORUM,
        "eq" => ::Hastur::Cassandra::CONSISTENCY_EACH_QUORUM,
        "each_quorum" => ::Hastur::Cassandra::CONSISTENCY_EACH_QUORUM,
        "each" => ::Hastur::Cassandra::CONSISTENCY_EACH_QUORUM,
        "each quorum" => ::Hastur::Cassandra::CONSISTENCY_EACH_QUORUM,
        "all" => ::Hastur::Cassandra::CONSISTENCY_ALL,
        "any" => ::Hastur::Cassandra::CONSISTENCY_ANY,
      }

      def param_consistency
        consistency = CONSISTENCY_PARAMS[params[:consistency].downcase]
        raise "Unknown cassandra consistency #{params[:consistency].inspect}!" unless consistency
        consistency
      end

      #
      # Evaluate HTTP query parameters and build a hash of Cassandra query parameters, then return a list
      # of per message name option hashes based off that. The list is necessary for column range queries
      # that are passed in the options hash to Hastur::Cassandra.get.
      #
      # "count" vs "limit" is an unfortunate naming situation. Cassandra uses "count" to mean "how many
      # results, maximum?"  We use it to mean "please return a count of my results."  We use "limit" for
      # Cassandra's "count". Cassandra uses get_count or count_columns for "please return a count of my
      # results."  I don't think we can win here, Cassandra-naming-wise.
      #
      # @param [Array<String>] want_names
      # @return [Array<Hash{Symbol => Object}>] options hash for Hastur::Cassandra.get
      #
      def build_name_option_list(want_names)
        cass_options = {}
        cass_options[:reversed] = true if param_is_true(params[:reversed])
        cass_options[:profiler] = true if param_is_true(params[:profiler])
        cass_options[:count] = params[:limit].to_i if params[:limit]
        cass_options[:consistency] = param_consistency if params[:consistency]
        cass_options[:request_ts] = env[:hastur_timestamp]

        case params[:kind]
        when "value"  ; cass_options[:value_only] = true
        when "rollup" ; cass_options[:rollup_only] = true
        when "count"  ; cass_options[:count_columns] = true
        end

        if params[:rollup_period] or params[:kind] == "rollup"
          unless ROLLUP_PERIODS.include?(params[:rollup_period])
            raise "Invalid or missing rollup period: #{params[:rollup_period].inspect}.  Should be one of: #{ROLLUP_PERIODS.join(",")}"
          end
          cass_options[:rollup_period] = params[:rollup_period]
        end

        if want_names.any?
          name_option_list = []

          want_names.each do |name|
            if name.include? '*'
              match = name.split('*')
              if name.start_with?('*')
                raise "Invalid name search '#{name}'. Suffix matching is not supported."
              else
                name_option_list << cass_options.merge(:name_prefix => match[0])
              end
            else
              name_option_list << cass_options.merge(:name => name)
            end
          end
          name_option_list
        else
          [ cass_options ]
        end
      end

      def output_operation(output, op_name, t)
        Hastur.gauge op_name, t, env[:hastur_timestamp]
        if output["profiler"]
          output["profiler"][op_name] = {
            env[:hastur_timestamp] => t
          }
        end
      end

      def timed_output_operation(output, op_name)
        t0 = Time.now
        yield
        t = ((Time.now - t0) * 1_000_000).to_i

        output_operation(output, op_name, t)
      end

      #
      # Actually query Hastur. The query is based on the Sinatra
      # params. Where appropriate, values can be comma-separated
      # lists.
      #
      # The params are query params, and so are supplied as strings.
      # "Boolean" in this case means the parameter may have a value
      # of "true", "false" or a few other things meaningfully.
      #
      # @param [Hash] params Options for how to query Hastur
      # @option params [String] kind What kind of data to return - message, value, count or rollup
      # @option params [String] uuid What UUID or comma-separated UUIDs to query
      # @option params [String] type What type or comma-separates types to query
      # @option params [String] name Message name or comma-separated names to query
      # @option params [String] limit Maximum number of messages per row to query
      # @option params [Boolean] reversed Limit messages in reversed (time-ascending) order
      # @option params [String] consistency Cassandra consistency to read at
      # @option params [Boolean] raw Return messages as escaped JSON in the output JSON
      # @option params [String] labels Filter on labels using label=<label>:<value>,... format, url encoded
      # @option params [Boolean] profiler Return profiling data with query results
      #
      def query_hastur(params)
        kind = params[:kind]
        types = type_list_from_string(params[:type])
        uuids = uuid_or_hostname_to_uuids params[:uuid].split(',')
        names = params[:name] ? params[:name].split(',') : []
        labels = params[:label] ? CGI::unescape(params[:label]).split(',') : []

        unless KINDS.include? kind
          hastur_error! "Illegal 'kind' output option: #{kind.inspect}", 404
        end

        if types.empty?
          return {}
        end

        unless types.all? { |t| TYPES[:all].include?(t) }
          hastur_error! "Invalid type(s): '#{types}', not all included in #{TYPES[:all].join(", ")}", 404
        end

        if labels.any?
          # flip kind to message when splitting on label, then convert back to
          # value format after the query
          if kind == "value"
            params[:kind] = "message"
          elsif kind != "message"
            hastur_error! "filtering on labels is only valid for /value and /message data queries", 404
          end
        end

        # Some message types are day bucketed and are only expected once a day, like registrations,
        # heartbeats, and ohai information. These should default to getting one day of data.
        if types & DEFAULT_DAY_BUCKET == types
          default_span = :one_day
        else
          default_span = :five_minutes
        end

        start_ts, end_ts = get_start_end default_span
        name_option_list = build_name_option_list names

        # query cassandra
        values = Hastur.time "hastur.rest.db.query_time" do
          name_option_list.map do |options|
            Hastur::Cassandra.get(cass_client, uuids, types, start_ts, end_ts, options)
          end
        end

        # Some queries go directly to a Cassandra range scan, which only matches prefixes
        # so a second pass is required to reduce the data down to only what was requested
        # for infix wildcards.  This should be done before expensive ops like label filters.
        if names.select {|n| n.include?('*') }.any?
          timed_output_operation(output, "hastur.rest.filter_names_time") do
            filter_out_unwanted_names output, names
          end
        end

        t0 = Time.now
        output = sort_series_keys(flatten_rows(values))
        output_operation(output, "hastur.rest.sort_keys", ((Time.now - t0).to_f * 1_000_000).to_i)

        if params[:kind] == "message"
          timed_output_operation(output, "hastur.rest.deserialize_time") do
            output = deserialize_json_messages(output)
          end
        end

        if labels.any?
          timed_output_operation(output, "hastur.rest.label_filter_time") do
            output = filter_by_label(output, labels)
          end

          if kind == "value" and params[:kind] == "message"
            timed_output_operation(output, "hastur.rest.message_conversion_time") do
              output = convert_messages_to_values(output)
            end
          end
        end

        if params[:fun]
          timed_output_operation(output, "hastur.rest.aggregation_time") do
            output = apply_functions(params[:fun], output)
          end
        end

        output
      end

      def query_hastur_by_labels(params)
        # Calculate query times with start, end, ago
        start_ts, end_ts = get_start_end :one_day

        # These are lists of sets to intersect to get the final sets.
        # Each entry on this list potentially cuts down the final
        # query set.
        query_uuids = [:all]
        query_types = [:all]
        query_names = [:all]

        must, must_not = parse_labels(params[:label])

        if params[:app]
          must["app"] = params["app"].strip
        end

        if must.empty? && !must_not.empty?
          hastur_error! "Unimplemented!  You have to specify at least one " +
            "required label to also specify a prevented label.", 404
        end

        if params[:type]
          type_set = params[:type].split(",").map(&:strip)
          query_types.push(type_set)
        end

        if params[:uuid]
          uuid_set = params[:uuid].split(",").map(&:strip).map(&:downcase)
          query_uuids.push(uuid_set)
        end

        data = Hastur::Cassandra.lookup_label_uuids(cass_client, must, start_ts, end_ts)

        data.each do |lname, sub_hash|
          sub_hash.each do |lvalue, uuids|
            # Remove non-matching label values with same prefix
            unless lvalue == must[lname] || name_matches?(lvalue, must[lname])
              sub_hash.delete(lvalue)
            end
          end

          # Having removed inapplicable entries, get the UUIDs for applicable ones.
          query_uuids.push sub_hash.values.inject([], &:concat)
        end

        # Look up UUIDs and message types for the given message name, if given.
        # Don't look up UUIDs for message names from labels -- those won't
        # help since we already have their UUID span.
        if params[:name]
          names = params[:name].split(',').map(&:strip)

          query_names.push names

          uuids = []
          types = []
          lookup_name(names, start_ts, end_ts).each do |item|
            uuids.push item[:uuid]
            types.push Hastur::Message.type_id_to_symbol(item[:type_id])
          end

          # Cut down to uuids and types that match these names
          query_uuids.push uuids
          query_types.push types
        end

        # A UUID label-index lookup should always return an explicit list.
        uuids = intersect_params(query_uuids)
        raise "Internal error!" if uuids == :all
        types = intersect_params(query_types)
        msg_names = intersect_params(query_names)

        # With the UUIDs known, look up stat names and types, and then timestamps.
        data = Hastur::Cassandra.lookup_label_stat_names(cass_client, uuids, must.merge(must_not),
                                                         start_ts, end_ts)
        data = clean_nonmatching_lookup(data, must, uuids, types, msg_names)
        data = Hastur::Cassandra.lookup_label_timestamps(cass_client, data, must_not.keys,
                                                         start_ts, end_ts)

        flat_result = query_row_col_from_cassandra(data, start_ts, end_ts)
      end

      def query_row_col_from_cassandra(data, start_ts, end_ts)
        options = build_name_option_list([])[0]

        output = data.flat_map do |type, data_by_type|
          Hastur::Cassandra.query_cassandra_by_type_rows_cols(cass_client, type, params[:kind],
                                                              data_by_type, options)
        end

        output = Hastur::Cassandra.convert_list_to_hastur_series(output, {}, start_ts, end_ts, options)

        output
      end

      #
      # Dump data from Hastur. The query is based on the Sinatra
      # params.  Where appropriate, values can be comma-separated
      # lists.  But little or no post-operation or formatting is
      # done on the results.
      #
      # Params can include the following:
      #
      # @param [Hash] params Options for how to query Hastur
      # @option params [String] kind What kind of data to return - message, value, count or rollup
      # @option params [String] uuid What UUID or comma-separated UUIDs to query.  Required.
      # @option params [String] type What type or comma-separates types to query.  Required.
      # @option params [String] name Message name or comma-separated names to query.  NO EMBEDDED WILDCARDS!
      # @option params [String] limit Maximum number of messages per row to query
      # @option params [Boolean] reversed Limit messages in reversed (time-ascending) order
      # @option params [String] consistency Cassandra consistency to read at
      #
      def dump_from_hastur(params)
        types = type_list_from_string(params[:type])
        uuids = uuid_or_hostname_to_uuids params[:uuid].split(',')
        names = params[:name] ? params[:name].split(',') : []
        labels = params[:label] ? CGI::unescape(params[:label]).split(',') : []

        return {} if types.empty?

        unless types.all? { |t| TYPES[:all].include?(t) }
          hastur_error! "Invalid type(s): '#{types}', not all included in #{TYPES[:all].join(", ")}", 404
        end

        # Some message types are day bucketed and are only expected once a day, like registrations,
        # heartbeats, and ohai information. These should default to getting one day of data.
        if types & DEFAULT_DAY_BUCKET == types
          default_span = :one_day
        else
          default_span = :five_minutes
        end

        start_ts, end_ts = get_start_end default_span
        name_option_list = build_name_option_list names

        # query cassandra
        values = Hastur.time "hastur.rest.db.query_time" do
          name_option_list.map do |options|
            dump = Hastur::Cassandra.dump(cass_client, uuids, types, start_ts, end_ts, options.merge(:cass_query_size => 100))
            dump.map { |item| item[2] }
          end.flatten
        end

        values
      end

      #
      # Apply Hastur aggregation functions. Sets up control information
      # used by the aggregations in some cases and runs the evaluation,
      # returning the transformed result.
      #
      # @param [String] fun aggregation function string, will be unescaped!
      # @param [Hash] series
      # @return [Hash] series
      #
      def apply_functions(fun, series)
        expr = CGI::unescape(fun)

        # pass values needed for hitting Cassandra in
        control = { :cass_client => cass_client, :exclude_uuids => ["profiler"], :no_barewords => true }
        control[:start_ts], control[:end_ts] = get_start_end :one_day

        Hastur::Aggregation.evaluate(expr, series, control)
      end

      #
      # Take a list of nodes, where the names may be UUIDs or network names and
      # return a list of just UUIDs. Hostnames that cannot be resolved are dropped
      # from the list.
      #
      # @param [Array<String>] nodes list of UUIDs and network names
      # @return [Array<String>] uuids list of 36-byte UUIDs
      #
      def uuid_or_hostname_to_uuids(nodes)
        # avoid the Cassandra lookup if all the nodes are already UUIDs
        return nodes unless nodes.reject { |node| Hastur::Util.valid_uuid?(node) }.any?

        # node registration is daily, bucket the lookup on day boundary if unspecified
        day_start_ts, day_end_ts = get_start_end :one_day

        uuid_lookup = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", day_start_ts, day_end_ts)

        nodes.flatten.map do |maybe_uuid|
          if Hastur::Util.valid_uuid?(maybe_uuid)
            maybe_uuid
          else
            uuid_lookup[maybe_uuid]
          end
        end.compact
      end

      #
      # We sometimes fetch multiple rows of data from Cassandra and the data returned
      # by Hastur::Cassandra.get isn't an exact match for the JSON format, so merge the rows
      # and drop the type information.
      #
      # Hastur::Cassandra.get returns the following format:
      #   { :uuid => { :type => { :name => { :timestamp => value/object } } } }
      # This REST API returns:
      #   { :uuid => { :name => { :timestamp => value/object } } }
      #
      # @param [Hash] values Hastur::Cassandra.get formatted hash
      # @return [Hash] Hastur V1 output hash
      #
      def flatten_rows(values)
        output = {}
        values.each do |values_for_name_opts|
          values_for_name_opts.each do |uuid, node_data|
            # hash1: {"gauge"=>{"hastur.agent.utime"=>{1338517798448399=>"{\"type\"
            output[uuid] ||= {}
            node_data.each do |type, ts_values|
              # ts_values maps { :name => { :timestamp => value/object } }
              # This will return a structure without the types.
              output[uuid].merge!(ts_values)
            end
          end
        end

        output
      end

      #
      # Due to the merging of series, there can be overlap and the results are always unsorted. Even though
      # JSON/javascript specify associative arrays as unordered, we try to deliver sorted results anyways.
      # We should probably drop this step and specify the V1 JSON format as unordered, but the expectation
      # has already been set with internal users.
      #
      # I tried both in-place modification and this version and was surprised to find copying to a whole
      # new top-level is measurably faster and as a bonus is a pure function.
      #
      # @param [Hash] Hastur V1 output hash
      # @return [Hash] same format, but with the all the series ordered
      #
      def sort_series_keys(values)
        output = {}
        values.each do |uuid, name_series|
          if special_name?(uuid)
            output[uuid] = values[uuid]
            next
          end

          output[uuid] = {}
          name_series.each do |name, series|
            output[uuid][name] = {}
            series.keys.sort.each do |ts|
              output[uuid][name][ts] = series[ts]
            end
          end
        end

        output
      end

      #
      # deserialize JSON messages in the return hash so the end-user can deserialize in one pass
      #
      # @param [Hash] Hastur V1 output hash
      # @return [Hash] same format, but with the all the series ordered
      #
      def deserialize_json_messages(data)
        output = {}
        data.each do |uuid, name_series|
          if special_name?(uuid)
            output[uuid] = data[uuid]
            next
          end

          output[uuid] = {}
          name_series.each do |name, series|
            output[uuid][name] = {}
            series.each do |ts, value|
              if value.kind_of?(Java::byte[])
                value = String.from_java_bytes(value)
              end
              # MultiJson gets really upset if you ask it to decode a ruby Hash that ends up
              # being stringified - TODO(al,2012-06-21) figure out why hashes are appearing in this data
              unless value.kind_of? String
                logger.debug "BUG: Got a ruby #{value.class} where a JSON string was expected: #{value.inspect}"
                next
              end

              begin
                output[uuid][name][ts] = MultiJson.load value
              rescue Exception => e
                hastur_error! "JSON parsing failed for stored message: #{value.inspect} #{e.inspect}", 501
              end
            end
          end
        end
        output
      end

      #
      # Iterate over all messages and filter on labels.
      #
      def filter_by_label(data, labels)
        # finish parsing the query string into two lookup hashes
        must = {}
        must_not = {}
        labels.each do |lv|
          label, value = lv.split ':', 2
          if label.start_with? '!'
            must_not[label.slice(1, label.length)] = value
          else
            must[label] = value
          end
        end

        # iterate over every item in the series and apply the filter in a very brutal manner
        # this could be a little more terse with in-place modification, but it copies to be
        # consistent with other filtering passes
        output = {}
        data.each do |uuid, name_series|
          if special_name?(uuid)
            output[uuid] = data[uuid]
            next
          end

          output[uuid] = {}
          name_series.each do |name, series|
            output[uuid][name] = {}
            series.each do |ts, value|
              labels = value["labels"]

              if must.none?
                output[uuid][name][ts] = value
              else
                must.each do |l,v|
                  if v.nil?
                    if labels.has_key?(l)
                      output[uuid][name][ts] = value
                    else
                      output[uuid][name].delete(ts)
                    end
                  elsif not labels.has_key?(l) or labels[l].to_s != v
                    output[uuid][name].delete ts
                  else
                    output[uuid][name][ts] = value
                  end
                end
              end

              unless must_not.none?
                must_not.each do |l,v|
                  if v.nil? and labels.has_key? l
                    output[uuid][name].delete ts
                  elsif labels[l] and labels[l].to_s == v
                    output[uuid][name].delete ts
                  end
                end
              end
            end
          end
        end
        output
      end

      #
      # When the user requests /value but filters on label, we fetch the messages
      # then need to convert back to value format, dumping most of the message.
      #
      def convert_messages_to_values(data)
        output = {}
        data.each do |uuid, name_series|
          if special_name?(uuid)
            output[uuid] = data[uuid]
            next
          end

          output[uuid] = {}
          name_series.each do |name, series|
            output[uuid][name] = {}
            series.each do |ts, value|
              output[uuid][name][ts] = value["value"]
            end
          end
        end
        output
      end

      #
      # Parse a name-type_id-uuid key and return it in 3 parts, handles names
      # with dashes in them by popping off the end after split.
      #
      # @param [String] key - key as stored in Cassandra
      # @return [Hash{:name => String, :type_id => Fixnum, :uuid => String}]
      # @example
      #   key = 'collectd.contextswitch-11-079c8b32-8a95-11e1-a1b9-123138124754'
      #   item = parse_name_lookup(key)
      #   {:name => 'collectd.contextswitch', :type_id => 11, :uuid => '079c8b32-8a95-11e1-a1b9-123138124754'}
      #
      def parse_name_lookup(key)
        # uuid & type_id are fixed format, names are not and may contain dashes,
        # so this has to work back-to-front to avoid breaking on names with dashes
        parts = key.split '-'
        uuid = parts.pop(5).join '-'
        type_id = parts.pop.to_i
        name = parts.join '-'
        { :name => name, :type_id => type_id, :uuid => uuid }
      end

      #
      # Check if a given string is a special top-level entry rather than a real UUID.
      #
      # @param [String] name to check
      # @return [Boolean] true if special rather than normal
      #
      def special_name?(name)
        name.to_s == "profiler"
      end

      #
      # Check if a given message name string matches the possibly wildcarded
      # match string. Does not use RE evaluation and is safe to use with query parameters.
      #
      # @param [String] name
      # @param [String] match either exact or wildcard match
      # @return [Boolean] true if matches
      #
      def name_matches?(name, match)
        if match.include? '*'
          parts = match.split '*'
          first = parts.shift

          # if it's a leading *, this works because start_with?("") always returns true
          # and has a length of 0 so the position stays at 0, which is correct
          if name.start_with?(first)
            # check for suffix match right away, accounting for a final * which split doesn't return
            if not match.end_with? '*' and not name.end_with?(parts.pop)
              return false
            end

            # check any internal wildcards
            position = first.length
            parts.each do |p|
              # find the substring starting at the position end of the last match
              found = name.index(p, position)
              if found and found >= position
                position = found + p.length # end of the matched substr
              else
                return false
              end
            end
          end
        elsif name == match
          true
        end
      end

      #
      # Modify the output hash in-place, deleting any name keys that don't match what the user requested.
      #
      # @param [Hash] output to be modified
      # @param [Array<String>] list of names
      #
      def filter_out_unwanted_names(output, names)
        names.each do |match|
          output.keys.each do |uuid|
            next if special_name?(uuid)

            output[uuid].keys.each do |name|
              unless name_matches?(name, match)
                output[uuid].delete name
              end
            end
          end
        end
      end

      #
      # Look up message names in the "name-" lookup_by_key row. Handles comma-separated lists.
      # @see parse_name_lookup
      #
      # @param [String] match_name the name to look up
      # @param [Fixnum] start_ts
      # @param [Fixnum] end_ts
      # @return [Array<Hash{Symbol => String,Fixnum}>]
      #
      def lookup_name(names, start_ts, end_ts)
        names_out = []
        lookup = Hastur::Cassandra.lookup_by_key(cass_client, "name", start_ts, end_ts)

        names.each do |match_name|
          # this will get slower as we get more names in the db, at which point we should
          # add prefix range querying to lookup_by_key where possible
          lookup.keys.map do |key|
            item = parse_name_lookup(key)

            if name_matches?(item[:name], match_name)
              names_out << item
            end
          end
        end
        names_out
      end

      #
      # Computes the request url without the path information
      #
      def root_uri
        uri = URI.parse request.url
        uri.path = ""
        uri.query = nil
        uri.to_s
      end

      #
      # Creates a cassandra client that connects as needed
      #
      # Expects the environment variable CASSANDRA_URIS to be set to a JSON array of
      # cassandra servers, defaults to 127.0.0.1:9160.
      #
      # If only one server is configured, node auto discovery is automatically disabled.
      #
      # @return [Cassandra] cassandra client object
      #
      def cass_client
        @cass_client ||= cass_java_client
      end

      private

      def cass_java_client
        require_relative "./cass_java_client"
        ::Hastur::API::CassandraJavaClient.new @cassandra_uris
      end

      public

      #
      # Get a logger handle from the framework.
      #
      # @return [Logger]
      #
      def logger
        @logger ||= Logger.new STDERR
      end

      #
      # Serialize output.
      # Uses params[:format], defaults to JSON.
      # If params[:cb] is set, output is JSONP with the specified callback.
      #
      # @param [Hash] content
      # @return [String] Serialized content
      #
      def serialize(content, params)
        t0 = Time.now

        if params[:format] == "csv"
          response['Content-Type'] = "text/csv"
          out = CSV.generate do |csv|
            csv << %w[node name timestamp value]
            content.each do |uuid, name_series|
              name_series.each do |name, ts_val|
                ts_val.each do |ts, val|
                  csv << [uuid, name, ts, val]
                end
              end
            end
          end
        # when the cb parameter is specified, return a JSONP response
        elsif params[:format] == "jsonp" or params[:cb]
          hastur_error!("cb callback parameter is required for jsonp!", 501) unless params[:cb]
          response['Content-Type'] = "text/javascript"
          out = "#{params[:cb]}(#{MultiJson.dump(content)});\n"
        # otherwise, just make it regular JSON
        else
          response['Content-Type'] = "application/json"
          out = MultiJson.dump(content, :pretty => params[:pretty]) + "\n"
        end

        t = Time.now - t0
        STDERR.puts "Time to serialize: #{t.to_f} seconds"

        out + "\n"
      end

      #
      # Calls through to the framework's error handlers with the provided information.
      # Calls halt().
      #
      def hastur_error!(code=501, message="FAIL", bt=[])
        error = {
          :status => code,
          :message => message,
          :backtrace => bt.kind_of?(Array) ? bt[0..10] : bt
        }

        # remove this after getting the loggers to do the right thing
        STDERR.puts MultiJson.dump(error, :pretty => true)

        error[:url] = request.url
        halt serialize(error, {})
      end

      #
      # Do an intersection between sets of param values (UUIDs, types, etc) and/or
      # :all, meaning "all possible values."
      #
      def intersect_params(lists)
        return [] if lists.empty?

        lists = lists.select { |l| l != :all }
        return :all if lists.empty?

        lists.inject(:"&")
      end

      #
      # Parse a Hastur-retrieval-format label param into
      # a set of "must" and "must not" label values.
      #
      def parse_labels(label_param)
        labels = CGI::unescape(label_param).split(',')

        must = {}
        must_not = {}
        labels.each do |lv|
          label, value = lv.split ':', 2
          if label.start_with? '!'
            must_not[label[1..-1]] = "*"
          else
            must[label] = value || ""
          end
        end

        [ must, must_not ]
      end

      #
      # This is used to clean data from lookup_label_stat_names of
      # entries not matching a restricted set of uuids, types or
      # message names.
      #
      def clean_nonmatching_lookup(data, must, uuids, types, msg_names)
        # Clean out non-matching label values
        data.each do |lname, lvalue_hash|
          lvalue_hash.keys.each do |lvalue|
            unless lvalue == must[lname] || name_matches?(lvalue, must[lname])
              lvalue_hash.delete(lvalue)
            end
          end
        end

        # Clean out non-matching types
        unless types == :all
          data.each do |lname, lvalue_hash|
            lvalue_hash.each do |lvalue, type_hash|
              type_hash.keys.each do |type|
                unless types.include? type
                  type_hash.delete(type)
                end
              end
            end
          end
        end

        # Clean out non-matching msg names
        unless msg_names == :all
          data.each do |lname, lvalue_hash|
            lvalue_hash.each do |lvalue, type_hash|
              type_hash.each do |type, msg_name_hash|
                msg_name_hash.keys.each do |msg_name|
                  unless msg_names.include?(msg_name)
                    msg_name_hash.delete msg_name
                  end
                end
              end
            end
          end
        end

        data
      end
    end
  end
end
