require "sinatra/base"

require "cgi"
require "hastur/api"
require "hastur-server/cassandra/schema"
require "hastur-server/cassandra/rollup"
require "hastur-server/time_util"
require "hastur-server/util"
require "hastur-server/aggregation"
require "hastur-server/api/constants"
require "hastur-server/api/helpers"
require "multi_json"
require "termite"

require "json" # This is the JRuby-JSON gem
MultiJson.use :json_gem

module Hastur
  module Service
    #
    # The Hastur Retrieval REST service.
    #
    # Parameters:
    #   * Timestamps are in microseconds since the Unix epoch
    #
    # Conventions:
    #   * use singular names for resources in all paths & params to be consistent
    #   * explicitly use the correct HTTP 1.1 status codes
    #   * support count and limit parameters everywhere sensible
    #
    # Extensions:
    #   * parameter values may be comma-delimited lists
    #
    # Future:
    #   * better query size limits / handling
    #   * add authentication
    #
    class RetrievalV2 < Sinatra::Base
      include Hastur::TimeUtil # import all the usec_* methods
      include Hastur::API::Constants
      helpers Hastur::API::Helpers

      def initialize(*args)
        if args[0].respond_to?(:each)
          cass_servers = args.unshift
          @cassandra_uris = cass_servers.flatten
        else
          @cassandra_uris = [ '127.0.0.1:9160' ]
        end

        super(*args)
      end

      configure do
        set :show_exceptions, false
        error(500) do
          e = env["sinatra.error"]
          if e.kind_of? Exception
            hastur_error! "Server exception: #{e}", 500, e.backtrace
          else
            hastur_error! "Server error. Either you found a bug or made a malformed request.", 500, e.backtrace
          end
        end
      end

      KNOWN_PARAMS = [ :name, :uuid, :type, :kind, :rollup_period, :ago, :cb, :pretty, :app, :hostname, :fun,
                       :start, :end, :consistency, :reversed, :profiler, :limit, :label, :format,
                       :details ].map(&:to_s)

      before do
        response['Access-Control-Allow-Origin'] = "*"
        Hastur.mark 'hastur.rest.uri', request.url

        unknown = params.keys - KNOWN_PARAMS
        unless unknown.empty?
          hastur_error! "Unknown parameters: #{unknown.join(',')}", 404
        end

        # grab a timestamp at the beginning of each request to use in repeated calls to get_start_end
        env[:hastur_timestamp] = Hastur.timestamp

        # default to pretty printing for non-XHR requests
        params[:pretty] = true unless request.xhr?

        params[:uuid].downcase! if params[:uuid]

        if params[:rollup_period] && !ROLLUP_PERIODS.include?(params[:rollup_period].to_s)
          hastur_error! "Given 'rollup_period' param '#{params[:rollup_period]}' " +
            "is not one of #{ROLLUP_PERIODS.join(", ")}.", 404
        end

        if params[:ago] && params[:ago] !~ /^\d+$/ && !Hastur::TimeUtil.time_intervals.include?(params[:ago])
          hastur_error! "Given 'ago' param '#{params[:ago]}' " +
            "is not one of #{Hastur::TimeUtil.time_intervals.join(", ")}.", 404
        end

        if params[:kind] && !KINDS.include?(params[:kind])
          hastur_error! "Given 'kind' param '#{params[:kind]}' is not one of #{KINDS.join(", ")}.", 404
        end

        if params[:format] && !FORMATS.include?(params[:format])
          hastur_error! "Given 'format' param '#{params[:format]}' is not one of #{FORMATS.join(", ")}.", 404
        end
      end

      #
      # @!method /v2
      #
      # Top-level resources.
      #
      # @return [Hash{String=>URI}] keys are names, values are resource URIs
      #
      get "/v2" do
        serialize({
          :lookup   => "#{root_uri}/v2/lookup",
          :query    => "#{root_uri}/v2/query",
        }, params)
      end

      #
      # @!method /v2/lookup
      #
      # Top-level resources.
      #
      # @return [Hash{String=>URI}] keys are names, values are resource URIs
      #
      get "/v2/lookup" do
        serialize({
          :type     => "#{root_uri}/v2/lookup/type",
          :app      => "#{root_uri}/v2/lookup/app",
          :node     => "#{root_uri}/v2/lookup/node",
          :name     => "#{root_uri}/v2/lookup/name",
          :hostname => "#{root_uri}/v2/lookup/hostname",
        }, params)
      end

      #
      # @!method /v2/lookup/type
      #
      # A structure of all the supported Hastur message types.
      #
      # @return [Hash{String=>Array<String>}]
      #
      get "/v2/lookup/type" do
        serialize TYPES, params
      end

      #
      # @!method /v2/lookup/app
      #
      # Retrieves all of the registered applications.
      #
      get "/v2/lookup/app" do
        start_ts, end_ts = get_start_end :one_day
        app_hash = Hastur::Cassandra.lookup_by_key cass_client, :app_name, start_ts, end_ts
        app_names = app_hash.map { |col_key, _| col_key[0..-38] }.uniq

        h = {}
        # Populate the return data object with the appropriate hash values
        app_names.each do |app|
          if app and app.length > 3
            h[app] = "#{root_uri}/v2/lookup/app/#{CGI.escape(app)}"
          end
        end

        serialize h, params
      end

      #
      # @!method /v2/lookup/app/:app
      #
      # Retrieves meta-data about a specific application name.
      #
      # @param app URL-encoded application name (required)
      #
      get "/v2/app/:app" do
        start_ts, end_ts = get_start_end :one_day
        app_name_data = Hastur::Cassandra.lookup_by_key cass_client, :app_name, start_ts, end_ts

        uuids_by_app_name = {}
        app_name_data.each do |col_key, _|
          app_name = col_key[0..-38]
          uuid = col_key[-36..-1]

          uuids_by_app_name[app_name] ||= []
          uuids_by_app_name[app_name] << uuid
        end

        array = params[:app].split(",").map do |app|
          bare_app = CGI.unescape(app)
          {
            :app     => bare_app,
            :node    => uuids_by_app_name[bare_app] || [],
          }
        end

        serialize array, params
      end

      #
      # @!method /v2/data/node
      #
      # Retrieves a list of currently registered Hastur-enabled nodes
      #
      get "/v2/lookup/node" do
        start_ts, end_ts = get_start_end :one_day
        uuid_hash = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts

        uuid_hash.keys.each do |uuid|
          uuid_hash[uuid] = "#{root_uri}/v2/lookup/node/#{uuid}"
        end

        serialize uuid_hash, params
      end

      #
      # @!method /v2/node/:uuid/:kind
      #
      # Retrieve a set of resources for the given UUID. Parameters may be
      # comma-separated values when specifying multiple of a given item.
      #
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      #
      get "/v2/lookup/node/:uuid" do
        start_ts, end_ts = get_start_end :one_day
        uuids = params[:uuid].split(",")

        # reduce the list of UUID's to those that have been seen in the provided time window
        # so invalid UUID's passed in on the URI don't cause bad queries
        seen_uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
        req_uuids = uuids & seen_uuids.keys

        if req_uuids.empty?
          hastur_error! "None of #{params[:uuid]} have sent any messages recently.", 404
        else
          array = req_uuids.map do |uuid|
            node = { :hostname => "#{root_uri}/v2/lookup/hostname/#{uuid}" }
            TYPES.each do |type,subtypes|
              if TYPES_WITH_VALUES.include?(type.to_s)
                node["#{type}/value"] = "#{root_uri}/v2/lookup/node/#{uuid}/type/#{type}/value"
              end
              node["#{type}/message"] = "#{root_uri}/v2/lookup/node/#{uuid}/type/#{type}/message"

              if type != :all
                subtypes.each do |subtype|
                  if TYPES_WITH_VALUES.include?(subtype.to_s)
                    node["#{subtype}/value"] = "#{root_uri}/v2/lookup/node/#{uuid}/type/#{subtype}/value"
                  end
                  node["#{subtype}/message"] = "#{root_uri}/v2/lookup/node/#{uuid}/type/#{subtype}/message"
                end
              end
            end
            node
          end
          serialize array, params
        end
      end

      #
      # @!method /v2/lookup/hostname/uuid
      #
      # Gets all network names / hostnames for all uuids.
      #
      # @param start Starting timestamp default 24hrs ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @return [Hash{String => String}] hash of network names known to Hastur
      #
      get "/v2/lookup/hostname/uuid" do
        start_ts, end_ts = get_start_end :one_day
        uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
        out = Hastur::Cassandra.network_names_for_uuids(cass_client, uuids.keys, start_ts, end_ts)
        serialize out, params
      end

      #
      # @!method /v2/lookup/hostname/uuid/:uuid
      #
      # Gets the various network names / hostnames for the given uuid(s).
      #
      # @param uuid UUID(s) to query for
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @return [Hash{String => String}] hash of network names known to Hastur
      # @example
      #   curl -s http://hastur/v2/lookup/hostname/uuid/4fb46081-f677-4604-878b-9d5b1f5addd9
      #   { :hostname => "gandalf", :fqdn => "gandalf.thewhite.com", :nodename => "gandalf.thewhite.com",
      #     :names => [ "gandalf", "gandalf.thewhite.com" ] }
      #
      get "/v2/lookup/hostname/uuid/:uuid" do
        start_ts, end_ts = get_start_end :one_day
        uuids = params[:uuid].split(",")
        out = Hastur::Cassandra.network_names_for_uuids(cass_client, uuids, start_ts, end_ts)
        serialize out, params
      end

      #
      # @!method /v2/lookup/uuid/hostname
      #
      # Fetch the whole hostname -> UUID lookup table. Primarily intended for the proxy
      # so it can cache it. Might be useful elsewhere.
      #
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @return [Hash{String => String}] hash of hostname => uuid
      # @example
      #   curl -s http://hastur/v2/lookup/uuid/hostname
      #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9",
      #    "frodo.shire.com":"e006683a-2bbd-4919-912a-07f64cbe7348", ...}
      #
      get "/v2/lookup/uuid/hostname" do
        start_ts, end_ts = get_start_end :one_day
        out = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)
        serialize out, params
      end

      #
      # @!method /v2/lookup/uuid/hostname/:hostname
      #
      # Get the UUID for a hostname. This relies on a quite a number of datapoints coming
      # together and is strictly best-effort. At a minimum the node will have to have registered
      # and ideally have a sane hostname to start with. The lookup table must also be up-to-date
      # and is managed by an external scheduler.
      #
      # @param hostname(s) to translate, a comma-separated list is allowed
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      #
      # @example
      #   curl -s http://hastur/v2/lookup/uuid/hostname/gandalf.thewhite.com
      #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9"}
      #
      # @example
      #   curl -s http://hastur/v2/lookup/uuid/hostname/gandalf.thewhite.com,frodo.shire.com
      #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9",
      #    "frodo.shire.com":"e006683a-2bbd-4919-912a-07f64cbe7348"}
      #
      get "/v2/lookup/uuid/hostname/:hostname" do
        start_ts, end_ts = get_start_end :one_day
        hostnames = params[:hostname].split(",")
        lookup = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)

        # just rely on the lookup table and sink most of the logic there in a scheduled job
        out = {}
        hostnames.each do |host|
          out[host] = lookup[host]
        end

        serialize out, params
      end

      #
      # @!method /v2/lookup/name
      #
      # Return all UUIDs and all names for that UUID in the same
      # { uuid => { name => {} } } structure as data.
      #
      # @param start Starting timestamp, default one day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param fun An aggregation function to apply before returning the data
      # @example
      #   http://hastur/v2/lookup/name
      #
      get "/v2/lookup/name" do
        start_ts, end_ts = get_start_end :one_day
        lookup = Hastur::Cassandra.lookup_by_key(cass_client, "name", start_ts, end_ts)
        out = {}

        lookup.keys.each do |key|
          info = parse_name_lookup(key)
          out[info[:uuid]] ||= {}
          out[info[:uuid]][info[:name]] = {}
        end

        if params[:fun]
          out = apply_functions(params[:fun], out)
        end

        serialize out, params
      end

      #
      # @!method /v2/lookup/name/:name
      #
      # Lookup all of the UUID's for a stat name.
      #
      # @param start Starting timestamp, default one day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param name Message name(s) to query for - supports wildcard suffix matching
      # @return [Hash{String => Array<String>}] hash of { "name" => [ "uuid" ] }
      # @example
      #   http://hastur/v2/lookup/name/awesome.things
      #   http://hastur/v2/lookup/name/awful.*
      #
      get "/v2/lookup/name/:name" do
        start_ts, end_ts = get_start_end :one_day
        names = {}
        lookup_name(params[:name].split(','), start_ts, end_ts).each do |item|
          if names.has_key? item[:name]
            names[item[:name]][item[:uuid]] = nil
          else
            names[item[:name]] = { item[:uuid] => nil }
          end
        end
        # convert the hash back to a unique array
        names.keys.each { |name| names[name] = names[name].keys }
        serialize names, params
      end

      #
      # @!method /v2/query
      #
      # Retrieve metrics by any queryable property.  Must specify one or
      # more of uuid, label, name or type.  All queries implicitly also
      # specify a time range, which defaults to five minutes.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type, default is "value"
      # @param format One of "csv", "json", "jsonp" for output format.  Default is "json".
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid Host UUID(s) to query for
      # @param type Message type(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param app Application name(s) to query for - supports wildcards
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra consistency to read at
      # @param raw don't merge messages into the return data, return it as escaped json inside the json
      # @param labels filter on labels using label=<label>:<value>,... format, url encoded
      #
      # @example
      #   http://hastur/v2/query?name=my.app.404&kind=value&ago=one_day
      #
      get "/v2/query" do
        params[:kind] ||= "value"

        unless params[:uuid] || params[:label] || params[:name] || params[:type]
          hastur_error! "Please supply at least one of uuid, label, name or type to /v2/query", 404
        end

        if (params[:label] && !params[:label].empty?) ||
            (params[:app] && !params[:app].empty?)
          return serialize query_hastur_by_labels(params), params
        end

        # Calculate query times with start, end, ago
        start_ts, end_ts = get_start_end :one_day

        # These are lists of sets to intersect to get the final sets.
        # Each entry on this list potentially cuts down the final
        # query set.
        query_uuids = [:all]
        query_types = [:all]

        if params[:type]
          type_set = params[:type].split(",").map(&:strip)
          query_types.push(type_set)
        end

        if params[:uuid]
          uuid_set = params[:uuid].split(",").map(&:strip).map(&:downcase)
          query_uuids.push(uuid_set)
        end

        # Look up UUIDs and message types for the given message name, if given.
        # Don't look up UUIDs for message names from labels -- those won't
        # help since we already have their UUID span.
        if params[:name]
          names = params[:name].split(',').map(&:strip)

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

        params[:uuid] = intersect_params(query_uuids).join(',')
        params[:type] = intersect_params(query_types).join(',')

        serialize query_hastur(params), params
      end

      #
      # @!method /v2/insert_message
      #
      # Insert a single Hastur-parseable JSON message.  This route
      # assumes that the message contains things like type and UUID
      # which can be extracted and used.
      #
      post "/v2/insert_message" do
        Hastur::Cassandra.insert(cass_client, params[:details], nil)
        "Ok\n"
      end

      #
      # @!method /v2/raw_dump
      #
      # Retrieve metrics in raw, simple form, not organized, ordered or formatted.
      # This query method requires UUIDs and does not use query indices.
      # Thus, app and label queries aren't permitted and name queries won't
      # automatically optimize based on what UUIDs have sent a given stat name
      # recently.
      #
      # Also, if the Hastur internal indices are wrong, this will still query
      # correctly, if stupidly.
      #
      # This route should be the very fastest way to dump a huge amount of
      # simply-structured data from Hastur.
      #
      # It can dump JSON messages separated by commas or values separated by
      # newlines, but not counts, rollups, etc.
      #
      # @param kind One of "message", "value", default is "value"
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid Host UUID(s) to query for - required
      # @param type Message type(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra consistency to read at
      #
      # @example
      #   http://hastur/v2/raw_dump?name=my.app.404&ago=one_day&uuid=47e88150-0102-0130-e57d-64ce8f3a9dc2
      #
      get "/v2/raw_dump" do
        params[:kind] ||= "value"

        unless ["value", "message"].include?(params[:kind])
          hastur_error! "Raw_dump may only return message or value, not '#{params[:kind]}'", 404
        end

        unless params[:uuid]
          hastur_error! "Raw_dump requires UUIDs since it uses no indices.", 404
        end

        unless params[:type]
          hastur_error! "You must give raw_dump at least one type.", 404
        end

        result = dump_from_hastur(params)

        result.join(params[:kind] == "value" ? "\n" : ",")
      end

      #
      # This is a proof-of-concept route before integrating this
      # functionality into the main query routines.
      #
      # TODO(noah): remove when integrated.
      #
      get "/v2/lookup/uuids_by_label" do
        start_ts, end_ts = get_start_end :one_day

        unless params[:label]
          hastur_error! "Must supply label(s) to uuids_by_label", 404
        end

        labels = CGI::unescape(params[:label]).split(',')

        must = {}
        must_not = {}
        labels.each do |lv|
          label, value = lv.split ':', 2
          if label.start_with? '!'
            must_not[label[1..-1]] = value || ""
          else
            must[label] = value || ""
          end
        end

        data = Hastur::Cassandra.lookup_label_uuids(cass_client, must, start_ts, end_ts)

        serialize data, params
      end

      #
      # This is a proof-of-concept route before integrating this
      # functionality into the main query routines.
      #
      # TODO(noah): remove when integrated.
      #
      get "/v2/lookup/stat_names_by_label" do
        start_ts, end_ts = get_start_end :one_day

        unless params[:label]
          hastur_error! "Must supply label(s) to stat_names_by_label", 404
        end

        unless params[:uuid]
          hastur_error! "Must supply UUID(s) to stat_names_by_label", 404
        end

        uuids = params[:uuid].split(",").map(&:strip).map(&:downcase)
        must, must_not = parse_labels params[:label]

        data = Hastur::Cassandra.lookup_label_stat_names(cass_client, uuids, must, start_ts, end_ts)

        serialize data, params
      end

      #
      # This is a proof-of-concept route before integrating this
      # functionality into the main query routines.
      #
      # TODO(noah): remove when integrated.
      #
      get "/v2/lookup/timestamps_by_label" do
        start_ts, end_ts = get_start_end :one_day

        unless params[:label]
          hastur_error! "Must supply label(s) to timestamps_by_label", 404
        end

        unless params[:uuid]
          hastur_error! "Must supply UUID(s) to timestamps_by_label", 404
        end

        uuids = params[:uuid].split(",").map(&:strip).map(&:downcase)
        must, must_not = parse_labels params[:label]

        # Do the two-level index lookup to get row keys and column keys
        data = Hastur::Cassandra.lookup_label_stat_names(cass_client, uuids, must.merge(must_not), start_ts, end_ts)
        data = Hastur::Cassandra.lookup_label_timestamps(cass_client, data, must_not.keys, start_ts, end_ts)

        data.inspect
      end

      #
      # @!method /statusz
      #
      # A simple health check that hits Cassandra.
      #
      get "/v2/statusz" do
        begin
          cass_client.status_check
          out = {}
          if cass_client.respond_to?(:ring)
            out = ring.map do |r|
              { :start_token => r.start_token, :end_token => r.end_token, :endpoints => r.endpoints }
            end
          end

          params[:pretty] = true
          serialize out, params
        #rescue Exception => e
        #  hastur_error! "Cassandra is not available.", 500, e.backtrace
        end
      end

      get "/v2/scala" do
        require "native_code"  # Use native_code.jar
        import "ScalaMain"

        sc_obj = ScalaMain.new

        "From scala: #{sc_obj.scala_string} / #{sc_obj.scala_computation(7)}\n"
      end

      #
      # Implement the Sinatra forward method so bad requests don't try to pass through
      # to the superclass and return 404 right away.
      #
      def forward
        hastur_error! "Invalid path: '#{request.path_info}'", 404
      end
    end
  end
end
