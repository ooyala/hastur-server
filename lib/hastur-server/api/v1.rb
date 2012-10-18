require "grape"
require "cgi"
require "hastur/api"
require "hastur-server/api/constants"
require "hastur-server/api/helpers"
require "hastur-server/cassandra/schema"
require "hastur-server/cassandra/derive"
require "hastur-server/time_util"
require "hastur-server/aggregation"

module Hastur
  module API
    #
    # The Hastur REST API Version 1.
    # This is almost exactly the same as the retrieval service, but uses Grape.
    #
    # Parameters:
    #   * Timestamps are in microseconds since the Unix epoch
    #   * relative time, e.g. "ago" may use: one_minute, one_hour, one_day, and two_days
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
    #   * fill in parameter checking
    #   * a more useful error handler
    #   * better query size limits / handling
    #   * enable authentication
    #   * maybe get Grape's XML output working
    #
    class V1 < Grape::API
      include Hastur::TimeUtil # import all the usec_* methods
      include Hastur::API::Constants
      helpers Hastur::API::Helpers

      version "v1", :using => :path
      default_format :json
      error_format :json
      rescue_from :all

      #
      # @!method /api
      # @todo this is broken, waiting on https://github.com/intridea/grape/issues/196
      #
      # Top-level resources.
      #
      # @return [Hash{String=>URI}] keys are names, values are resource URIs
      #
      desc "[broken] get a list of top-level resources"
      get do
        {
          :app    => "#{root_uri}/api/app",
          :type   => "#{root_uri}/api/type",
          :node   => "#{root_uri}/api/node",
        }
      end

      resource :app do
        #
        # @!method /api/app
        #
        # Retrieves all of the registered applications.
        #
        desc "get a list of registered apps as resources"
        get do
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

          h
        end

        #
        # @!method /api/app/:app
        #
        # Retrieves meta-data about a specific application name.
        #
        # @param app URL-encoded application name (required)
        #
        desc "retrieves metadata about a specific application by name"
        get "/:app" do
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

          array
        end
      end

      resource :lookup do
        resource :hostname do
          resource :uuid do
            #
            # @!method /api/lookup/hostname/uuid
            #
            # Gets all network names / hostnames for all uuids.
            #
            # @param start starting timestamp default 24hrs ago
            # @param end ending timestamp, default now
            # @param ago how many microseconds back to query - an alternative to start/end
            # @return [Hash{String => String}] hash of network names known to Hastur
            #
            desc "get all network names for all uuids in a big associative array"
            get do
              start_ts, end_ts = get_start_end :one_day
              uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
              Hastur::Cassandra.network_names_for_uuids(cass_client, uuids.keys, start_ts, end_ts)
            end

            #
            # @!method /api/lookup/hostname/uuid/:uuid
            #
            # Gets the various network names / hostnames for the given uuid(s).
            #
            # @param uuid UUID(s) to query for
            # @param start starting timestamp, default 5 minutes ago unless querying only
            #   registration and info types, then default 1 day ago
            # @param end ending timestamp, default now
            # @param ago how many microseconds back to query - an alternative to start/end
            # @return [Hash{String => String}] hash of network names known to Hastur
            # @example
            #   curl -s http://hastur/api/lookup/hostname/uuid/4fb46081-f677-4604-878b-9d5b1f5addd9
            #   { :hostname => "gandalf", :fqdn => "gandalf.thewhite.com", :nodename => "gandalf.thewhite.com",
            #     :names => [ "gandalf", "gandalf.thewhite.com" ] }
            #
            params do
              requires :uuid, :type => String, :regexp => UUID_RE
            end
            desc "get all network names for the given uuid(s)"
            get "/:uuid" do
              start_ts, end_ts = get_start_end :one_day
              uuids = params[:uuid].split(",")
              Hastur::Cassandra.network_names_for_uuids(cass_client, uuids, start_ts, end_ts)
            end
          end
        end

        resource :uuid do
          resource :hostname do
            #
            # @!method /api/lookup/uuid/hostname
            #
            # Fetch the whole hostname -> UUID lookup table. Primarily intended for the proxy
            # so it can cache it. Might be useful elsewhere.
            #
            # @param start starting timestamp, default 5 minutes ago unless querying only
            #   registration and info types, then default 1 day ago
            # @param end ending timestamp, default now
            # @param ago how many microseconds back to query - an alternative to start/end
            # @return [Hash{String => String}] hash of hostname => uuid
            # @example
            #   curl -s http://hastur/api/lookup/uuid/hostname
            #   {"gandalf.thewhite.com":"4fb46081-f677-4604-878b-9d5b1f5addd9",
            #    "frodo.shire.com":"e006683a-2bbd-4919-912a-07f64cbe7348", ...}
            #
            desc "get a networm name to uuid lookup hash with all known network names as keys"
            get do
              start_ts, end_ts = get_start_end :one_day
              Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)
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
            # @param start starting timestamp, default 5 minutes ago unless querying only
            #   registration and info types, then default 1 day ago
            # @param end ending timestamp, default now
            # @param ago how many microseconds back to query - an alternative to start/end
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
            params do
              requires :hostname, :type => String, :regexp => UUID_OR_HOST_RE
            end
            desc "look up a hostname(s) and get its uuid"
            get "/:hostname" do
              start_ts, end_ts = get_start_end :one_day
              hostnames = params[:hostname].split(",")
              lookup = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)

              # just rely on the lookup table and sink most of the logic there in a scheduled job
              out = {}
              hostnames.each do |host|
                out[host] = lookup[host]
              end

              out
            end
          end
        end

        resource :name do
          #
          # @!method /api/lookup/name
          #
          # Return all UUIDs and all names for that UUID in the same
          # { uuid => { name => {} } } format as data.
          #
          # @param start starting timestamp, default one day ago
          # @param end ending timestamp, default now
          # @param ago how many microseconds back to query - an alternative to start/end
          # @param fun an aggregation function to apply before returning the data
          # @example
          #   http://hastur/api/lookup/name
          #
          desc "get a hash with all uuids and all names associated with those uuids"
          get do
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

            out
          end

          #
          # @!method /api/lookup/name/:name
          #
          # Lookup all of the UUID's for a stat name.
          #
          # @param start starting timestamp, default one day ago
          # @param end ending timestamp, default now
          # @param ago how many microseconds back to query - an alternative to start/end
          # @param name message name(s) to query for - supports wildcard suffix matching
          # @return [Hash{String => Array<String>}] hash of { "name" => [ "uuid" ] }
          # @example
          #   http://hastur/api/lookup/name/awesome.things
          #   http://hastur/api/lookup/name/awful.*
          #
          desc "find all uuids reporting the given stat name(s)"
          get "/:name" do
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
            names
          end
        end
      end

      resource :name do
        #
        # @!method /api/name/:name/:kind
        #
        # Retrieve metrics by stat name.
        #
        # @param kind one of "message", "value", "count" or "rollup" for data type
        # @param start starting timestamp, default 5 minutes ago
        # @param end ending timestamp, default now
        # @param ago how many microseconds back to query - an alternative to start/end
        # @param name message name(s) to query for - supports wildcards
        # @param uuid optional UUID(s) to query for
        # @param type optional message type(s) to query for
        # @param limit maximum number of values to return
        # @param reversed return earliest first instead of latest first
        # @example
        #   http://hastur/api/name/my.app.404/value
        #   http://hastur/api/name/my.app.*/message?ago=one_day
        #
        desc "retrieve data across all nodes by stat name(s)"
        get "/:name/:kind" do
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

          query_hastur params
        end
      end

      resource :type do
        #
        # @!method /api/type
        #
        # A structure of all the supported Hastur message types.
        #
        # @return [Hash{String=>Array<String>}]
        #
        desc "list all of the message types supported by Hastur"
        get do
          TYPES
        end

        #
        # @!method /api/type/:type
        #
        # Retrieve by type. Please be mindful of how much data this can end up fetching,
        # which is an awful lot for busy types like mark, counter, and gauge.
        #
        # @param start starting timestamp, default 5 minutes ago
        # @param end ending timestamp, default now
        # @param ago how many microseconds back to query - an alternative to start/end
        # @param uuid optional UUID(s) to query for
        # @param name optional message name(s) to query for - supports wildcards
        # @param limit maximum number of values to return
        # @param reversed return earliest first instead of latest first
        # @example
        #   http://hastur/api/type/event?ago=one_minute
        #   http://hastur/api/type/mark
        #
        desc "retrieve data by metric type(s)"
        get "/:type" do
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

          query_hastur params
        end
      end

      resource :node do
        #
        # @!method /api/node
        #
        # Retrieves a list of currently registered Hastur-enabled nodes
        #
        desc "get a list of registered nodes as resources"
        get do
          start_ts, end_ts = get_start_end :one_day
          uuid_hash = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts

          uuid_hash.keys.each do |uuid|
            uuid_hash[uuid] = "#{root_uri}/api/node/#{uuid}"
          end

          uuid_hash
        end

        #
        # @!method /api/node/:uuid
        #
        # Retrieve a set of resources for the given UUID. Parameters may be
        # comma-separated values when specifying multiple of a given item.
        #
        # @param start starting timestamp, default 5 minutes ago
        # @param end ending timestamp, default now
        # @param ago how many microseconds back to query - an alternative to start/end
        # @param uuid UUID(s) to query for
        #
        desc "get a list of resources for the given node(s)"
        get "/:uuid" do
          start_ts, end_ts = get_start_end :one_day
          uuids = params[:uuid].split(",")

          # reduce the list of UUID's to those that have been seen in the provided time window
          # so invalid UUID's passed in on the URI don't cause bad queries
          seen_uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
          req_uuids = uuids & seen_uuids.keys

          if req_uuids.empty?
            error! "None of #{params[:uuid]} have sent any messages recently.", 404
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
            array
          end
        end

        #
        # @!method /api/node/:uuid/.../:kind
        #
        # Retrieve Hastur messages by UUID. One or both of "name" or "type" is required in the path
        # or in query parameters. The most specific version of this resource is will provide the
        # best chance for the service to optimize the query.
        # Parameters may be comma-separated values when specifying multiple of a given item.
        #
        # @param kind one of "message", "value", "count" or "rollup" for data type
        # @param start starting timestamp, default 5 minutes ago unless querying only
        #   registration and info types, then default 1 day ago
        # @param end ending timestamp, default now
        # @param ago how many microseconds back to query - an alternative to start/end
        # @param uuid UUID(s) to query for
        # @param name message name(s) to query for - supports wildcards
        # @param type message type(s) to query for
        # @param limit maximum number of values to return
        # @param reversed return earliest first instead of latest first
        # @example
        #   /api/node/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/type/heartbeat/name/hastur.agent.heartbeat/value
        #   /api/node/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/name/hastur.agent.heartbeat/value
        #   /api/node/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/type/heartbeat/value
        #   /api/node/:uuid/type/heartbeat,mark/value
        #   /api/node/:uuid/name/hastur.agent.heartbeat,process_heartbeat/value
        #
        #
        desc "get data by node and type or name (or both)"
        get %w[/:uuid/:k1/:v1/:kind /:uuid/:k1/:v1/:k2/:v2/:kind] do
          if params[:k1] and %w[type name].include?(params[:k1])
            params[params.delete(:k1).to_sym] = params.delete(:v1)
          end

          if params[:k2] and %w[type name].include?(params[:k2])
            params[params.delete(:k2).to_sym] = params.delete(:v2)
          end

          query_hastur params
        end
      end

      #
      # @!method /statusz
      #
      # A simple health check that hits Cassandra.
      #
      desc "health check"
      get "/statusz" do
        begin
          # cass_client.ring will fail if no successful queries have run
          cass_client.get "gauge_archive", " "
          ring = cass_client.ring
          out = ring.map do |r|
            { :start_token => r.start_token, :end_token => r.end_token, :endpoints => r.endpoints }
          end
          params[:pretty] = true
          out
        rescue Exception => e
          error! "Cassandra is not available.", 500
        end
      end
    end
  end
end

