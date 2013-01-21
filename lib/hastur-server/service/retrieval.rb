require "sinatra/base"

require "cassandra/1.0"
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

#
# Do you need a v1 retrieval API?  Then you should rewind the git
# repo back to when it existed, worked, and ran on MRI Ruby rather
# than JRuby.  This is no longer tested, and in several ways is
# guaranteed to not work, or just have various differences.
#
# Since it exists at all for API compatibility reasons, various
# differences is not a good thing.
#
raise "This no longer works in current JRuby Hastur!"

# TODO(noah): Add parameter validation - alert on bad params?

# TODO(noah): Override for JRuby
MultiJson.use :yajl

module Hastur
  module Service
    #
    # The Hastur Retrieval REST service.
    # Extra formats and anything not directly related to querying Cassandra is in
    # the rest_proxy service.
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
    class Retrieval < Sinatra::Base
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
            hastur_error! "Server error. Either you found a bug or made a malformed request.", 500, bt
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
      end

      #
      # @!method /api
      #
      # Top-level resources.
      #
      # @return [Hash{String=>URI}] keys are names, values are resource URIs
      #
      get "/api" do
        serialize({
          :type     => "#{root_uri}/api/type",
          :app      => "#{root_uri}/api/app",
          :node     => "#{root_uri}/api/node",
        }, params)
      end

      #
      # @!method /api/type
      #
      # A structure of all the supported Hastur message types.
      #
      # @return [Hash{String=>Array<String>}]
      #
      get "/api/type" do
        serialize TYPES, params
      end

      #
      # @!method /api/app
      #
      # Retrieves all of the registered applications.
      #
      get "/api/app" do
        start_ts, end_ts = get_start_end :one_day
        app_hash = Hastur::Cassandra.lookup_by_key cass_client, :app_name, start_ts, end_ts
        app_names = app_hash.map { |col_key, _| col_key[0..-38] }.uniq

        h = {}
        # Populate the return data object with the appropriate hash values
        app_names.each do |app|
          if app and app.length > 3
            h[app] = "#{root_uri}/api/app/#{CGI.escape(app)}"
          end
        end

        serialize h, params
      end

      #
      # @!method /api/app/:app
      #
      # Retrieves meta-data about a specific application name.
      #
      # @param app URL-encoded application name (required)
      #
      get "/api/app/:app" do
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
      # @!method /api/node
      #
      # Retrieves a list of currently registered Hastur-enabled nodes
      #
      get "/api/node" do
        start_ts, end_ts = get_start_end :one_day
        uuid_hash = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts

        uuid_hash.keys.each do |uuid|
          uuid_hash[uuid] = "#{root_uri}/api/node/#{uuid}"
        end

        serialize uuid_hash, params
      end

      #
      # @!method /api/node/:uuid
      #
      # Retrieve a set of resources for the given UUID. Parameters may be
      # comma-separated values when specifying multiple of a given item.
      #
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      #
      get "/api/node/:uuid" do
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
            node = { :hostname => "#{root_uri}/api/lookup/hostname/#{uuid}" }
            TYPES.each do |type,subtypes|
              if TYPES_WITH_VALUES.include?(type.to_s)
                node["#{type}/value"] = "#{root_uri}/api/node/#{uuid}/type/#{type}/value"
              end
              node["#{type}/message"] = "#{root_uri}/api/node/#{uuid}/type/#{type}/message"

              if type != :all
                subtypes.each do |subtype|
                  if TYPES_WITH_VALUES.include?(subtype.to_s)
                    node["#{subtype}/value"] = "#{root_uri}/api/node/#{uuid}/type/#{subtype}/value"
                  end
                  node["#{subtype}/message"] = "#{root_uri}/api/node/#{uuid}/type/#{subtype}/message"
                end
              end
            end
            node
          end
          serialize array, params
        end
      end

      #
      # @!method /api/lookup/hostname/uuid
      #
      # Gets all network names / hostnames for all uuids.
      #
      # @param start Starting timestamp default 24hrs ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @return [Hash{String => String}] hash of network names known to Hastur
      #
      get "/api/lookup/hostname/uuid" do
        start_ts, end_ts = get_start_end USEC_ONE_DAY * 15
        uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
        out = Hastur::Cassandra.network_names_for_uuids(cass_client, uuids.keys, start_ts, end_ts)
        serialize out, params
      end

      #
      # @!method /api/lookup/hostname/uuid/:uuid
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
      #   curl -s http://hastur/api/lookup/hostname/uuid/4fb46081-f677-4604-878b-9d5b1f5addd9
      #   { :hostname => "gandalf", :fqdn => "gandalf.thewhite.com", :nodename => "gandalf.thewhite.com",
      #     :names => [ "gandalf", "gandalf.thewhite.com" ] }
      #
      get "/api/lookup/hostname/uuid/:uuid" do
        start_ts, end_ts = get_start_end USEC_ONE_DAY * 15
        uuids = params[:uuid].split(",")
        out = Hastur::Cassandra.network_names_for_uuids(cass_client, uuids, start_ts, end_ts)
        serialize out, params
      end

      #
      # @!method /api/lookup/uuid/hostname
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
      #   curl -s http://hastur/api/lookup/uuid/hostname
      #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9",
      #    "frodo.shire.com":"e006683a-2bbd-4919-912a-07f64cbe7348", ...}
      #
      get "/api/lookup/uuid/hostname" do
        start_ts, end_ts = get_start_end :one_day
        out = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)
        serialize out, params
      end

      #
      # @!method /api/lookup/uuid/hostname/:hostname
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
      #   curl -s http://hastur/api/lookup/uuid/hostname/gandalf.thewhite.com
      #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9"}
      #
      # @example
      #   curl -s http://hastur/api/lookup/uuid/hostname/gandalf.thewhite.com,frodo.shire.com
      #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9",
      #    "frodo.shire.com":"e006683a-2bbd-4919-912a-07f64cbe7348"}
      #
      get "/api/lookup/uuid/hostname/:hostname" do
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
      # @!method /api/lookup/name
      #
      # Return all UUIDs and all names for that UUID in the same
      # { uuid => { name => {} } } structure as data.
      #
      # @param start Starting timestamp, default one day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param fun An aggregation function to apply before returning the data
      # @example
      #   http://hastur/api/lookup/name
      #
      get "/api/lookup/name" do
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
      # @!method /api/lookup/name/:name
      #
      # Lookup all of the UUID's for a stat name.
      #
      # @param start Starting timestamp, default one day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param name Message name(s) to query for - supports wildcard suffix matching
      # @return [Hash{String => Array<String>}] hash of { "name" => [ "uuid" ] }
      # @example
      #   http://hastur/api/lookup/name/awesome.things
      #   http://hastur/api/lookup/name/awful.*
      #
      get "/api/lookup/name/:name" do
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
      # @!method /api/name/:name/:kind
      #
      # Retrieve metrics by stat name.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param name Message name(s) to query for - supports wildcards
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @example
      #   http://hastur/api/name/my.app.404/value
      #   http://hastur/api/name/my.app.*/message?ago=one_day
      #
      get "/api/name/:name/:kind" do
        name_start_ts, name_end_ts = get_start_end :one_day
        uuids = {}
        types = {}
        lookup_name(params[:name].split(','), name_start_ts, name_end_ts).each do |item|
          uuids[item[:uuid]] = nil
          types[Hastur::Message.type_id_to_symbol(item[:type_id])] = nil
        end

        # interestingly, allow query params to cut down the uuid/type
        params[:uuid] ||= uuids.keys.join ','
        params[:type] ||= types.keys.join ','

        serialize query_hastur(params), params
      end

      #
      # @!method /api/type/:type
      #
      # Retrieve by type.
      #
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param name Message name(s) to query for - supports wildcards
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @example
      #   http://hastur/api/type/event?ago=one_minute
      #   http://hastur/api/type/mark
      #
      get "/api/type/:type" do
        name_start_ts, name_end_ts = get_start_end :one_day
        lookup = Hastur::Cassandra.lookup_by_key(cass_client, "name", name_start_ts, name_end_ts)

        type_ids = params[:type].split(',').map do |type|
          TYPES.values.flatten.include?(type) ? Hastur::Message.symbol_to_type_id(type.to_sym) : nil
        end.compact

        uuids = lookup.keys.map do |key|
          item = parse_name_lookup(key)
          type_ids.include?(item[:type_id]) ? item[:uuid] : nil
        end

        params[:uuid] = uuids.uniq.compact.join ','
        params[:kind] = "message"

        serialize query_hastur(params), params
      end

      #
      # @!method /api/node/:uuid/type/:type/:kind
      #
      # Retrieve Hastur messages by UUID & type.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      #
      get "/api/node/:uuid/type/:type/:kind" do
        serialize query_hastur(params), params
      end

      #
      # @!method /api/node/:uuid/name/:name/:kind
      #
      # Retrieve Hastur messages by UUID & message name.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      #
      get "/api/node/:uuid/name/:name/:kind" do
        serialize query_hastur(params), params
      end

      #
      # @!method /api/node/:uuid/type/:type/name/:name/:kind
      #
      # Retrieve Hastur messages by UUID, type, and message name.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      #
      get "/api/node/:uuid/type/:type/name/:name/:kind" do
        serialize query_hastur(params), params
      end

      #
      # @!method /api/node/:uuid/type/:type/:kind
      #
      # Retrieve Hastur messages by UUID and type.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param kind One of "message", "value", "count" or "rollup" for output data type
      # @param start Starting timestamp, default 5 minutes ago unless querying only
      #   registration and info types, then default 1 day ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      #
      get "/api/node/:uuid/type/:type/:kind" do
        serialize query_hastur(params), params
      end

      #
      # @!method /statusz
      #
      # A simple health check that hits Cassandra.
      #
      get "/api/statusz" do
        begin
          # cass_client.ring will fail if no successful queries have run
          cass_client.get "gauge_archive", " "
          ring = cass_client.ring
          out = ring.map do |r|
            { :start_token => r.start_token, :end_token => r.end_token, :endpoints => r.endpoints }
          end
          params[:pretty] = true
          serialize out, params
        rescue Exception => e
          hastur_error! "Cassandra is not available.", 500, e.backtrace
        end
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
