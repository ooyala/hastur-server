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

      before do
        response['Access-Control-Allow-Origin'] = "*"
        Hastur.mark 'hastur.rest.uri', request.url

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
          :uuid     => "#{root_uri}/v2/lookup/uuid",
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
      # Retrieve metrics by any queryable property.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type, default is "value"
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

        # Got the UUID already?  Use the happy path with no
        # extra lookups.  This query will be maximally speedy.
        if params[:uuid]
          return serialize query_hastur(params), params
        end

        # Calculate query times with start, end, ago
        name_start_ts, name_end_ts = get_start_end :one_day

        query_uuids = []
        query_types = []

        query_types.push(params[:type].split(",").map(&:strip)) if params[:type]

        # Look up UUIDs and message types for the given message name, if present
        if params[:name]
          uuids = {}
          types = {}
          lookup_name(params[:name].split(','), name_start_ts, name_end_ts).each do |item|
            uuids[item[:uuid]] = nil
            types[Hastur::Message.type_id_to_symbol(item[:type_id])] = nil
          end

          # Allow query params to cut down the uuids and types arrays
          query_uuids.push uuids.keys
          query_types.push types.keys
        end

        params[:uuid] = query_uuids.join(',')
        params[:type] = query_types.join(',')

        serialize query_hastur(params), params
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
