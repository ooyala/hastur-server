require "grape"
require "cassandra/1.0"
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

        if params[:start]
          start_ts = Hastur.timestamp(params[:start].to_i)
        elsif params[:ago]
          start_ts = now - usec_from_interval(params[:ago])
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
        cass_options[:count] = params[:limit].to_i if params[:limit]

        case params[:kind]
        when "value"  ; cass_options[:value_only] = true
        when "rollup" ; cass_options[:rollup_only] = true
        when "count"  ; cass_options[:count_columns] = true
        end

        if params[:rollup_period] or params[:kind] == "rollup"
          unless ROLLUP_PERIODS.include?(params[:rollup_period])
            raise "Invalid or missing rollup period: #{params[:rollup_period].inspect}"
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

      #
      # Actually query Hastur. The query is based on the Sinatra
      # params. Where appropriate, values can be comma-separated
      # lists.
      #
      # Params can include the following:
      #
      # "kind" - what kind of data to return - message, value, count or rollup
      # "uuid" - uuid or list of uuids
      # "type" - type or list of types
      # "name" - message name or list of message names (can append * for match-all)
      # "reversed" - return results in reverse order - only matters with "limit"
      # "limit" - max number of results to return
      # "raw" - don't merge messages into the return data, return it as escaped json inside the json
      #
      def query_hastur(params)
        unless FORMATS.include?(params[:kind])
          hastur_error! "Illegal output option: #{params[:kind].inspect}", 404
        end

        types = type_list_from_string(params[:type])
        uuids = uuid_or_hostname_to_uuids params[:uuid].split(',')
        names = params[:name] ? params[:name].split(',') : []

        unless types.any? { |t| TYPES[:all].include?(t) }
          hastur_error! "Invalid type(s): '#{types}'", 404
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

        if FORMATS.include?(params[:kind])
          output = sort_series_keys(flatten_rows(values))

          if params[:kind] == "message"
            output = deserialize_json_messages(output)
          end
        else
          hastur_error! "Unsupported data type: #{params[:kind].inspect}!", 404
        end

        # Some queries go directly to a Cassandra range scan, which only matches prefixes
        # so a second pass is required to reduce the data down to only what was requested
        # for infix wildcards.
        if names.select {|n| n.include?('*') }.any?
          filter_out_unwanted_names output, names
        end

        if params[:fun]
          output = apply_functions(params[:fun], output)
        end

        output
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
        control = { :cass_client => cass_client }
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

      def flatten_to_array(values)
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
          output[uuid] = {}
          name_series.each do |name, series|
            output[uuid][name] = {}
            series.each do |ts, value|
              # MultiJson gets really upset if you ask it to decode a ruby Hash that ends up
              # being stringified - TODO(al,2012-06-21) figure out why hashes are appearing in this data
              unless value.kind_of? String
                logger.debug "BUG: Got a ruby hash where a JSON string was expected."
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
            output[uuid].keys.each do |name|
              unless name_matches?(name, match)
                output[uuid].delete name
              end
            end
          end
        end
      end

      #
      # Look up message names in the "name-" LookupByKey row. Handles comma-separated lists.
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
        unless @cass_client
          uri_json = ENV['CASSANDRA_URIS'] || '["127.0.0.1:9160"]'
          @cassandra_uris = MultiJson.load uri_json

          @cass_client = ::Cassandra.new("Hastur", @cassandra_uris, THRIFT_OPTIONS)

          # for non-production and port-forwarded ssh, there will only be one URI and it
          # should not auto-discover nodes
          if @cassandra_uris.one?
            @cass_client.disable_node_auto_discovery!
          end
        end
        @cass_client
      end

      ## The remaining methods have extra logic to support both Sinatra and Grape

      #
      # Get a logger handle from the framework.
      #
      # @return [Logger]
      #
      def logger
        if self.is_a? Grape::API
          API.logger
        else
          @logger ||= Logger.new
        end
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
        # when the cb parameter is specified, return a JSONP response
        if params[:format] == "csv"
          response['Content-Type'] = "text/csv"
          CSV.generate do |csv|
            csv << %w[node name timestamp value]
            content.each do |uuid, name_series|
              name_series.each do |name, ts_val|
                ts_val.each do |ts, val|
                  csv << [uuid, name, ts, val]
                end
              end
            end
          end
        elsif params[:format] == "jsonp" or params[:cb]
          hastur_error!("cb callback parameter is required for jsonp!", 501) unless params[:cb]
          response['Content-Type'] = "text/javascript"
          "#{params[:cb]}(#{MultiJson.dump(content)});\n"
        # otherwise, just make it regular JSON
        else
          response['Content-Type'] = "application/json"
          MultiJson.dump(content, :pretty => params[:pretty]) + "\n"
        end
      end

      #
      # Calls through to the framework's error handlers with the provided information.
      # Throws :error for Grape and calls halt() for Sinatra.
      #
      def hastur_error!(code=501, message="FAIL", bt=[])
        error = {
          :status => code,
          :message => message,
          :backtrace => bt.kind_of?(Array) ? bt[0..10] : bt
        }

        # remove this after getting the loggers to do the right thing
        STDERR.puts MultiJson.dump(error, :pretty => true)

        if self.is_a? Grape::API
          throw :error, error
        elsif self.is_a? Sinatra::Base
          error[:url] = request.url
          halt serialize(error, {})
        else
          abort "BUG: not a Grape::API or Sinatra::Base"
        end
      end
    end
  end
end
