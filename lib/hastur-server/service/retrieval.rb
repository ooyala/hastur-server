require "sinatra/base"

require "cassandra/1.0"
require "cgi"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "multi_json"
require "termite"

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

      #
      # All of the Hastur message types. These are used in various places in the API
      # usually in the :type field. The keys may be used to indicate that you want all
      # of the values, so for example, "stat" will get you all counters, gauges, and marks.
      #
      TYPES = {
        :metric       => %w[counter gauge mark compound],
        :heartbeat    => %w[hb_process hb_agent hb_pluginv1],
        :event        => %w[event],
        :log          => %w[log],
        :error        => %w[error],
        :registration => %w[reg_agent reg_process reg_pluginv1],
        :info         => %w[info_agent info_process info_ohai],
        :all          => %w[counter gauge mark compound
                            hb_process hb_agent hb_pluginv1
                            event log error
                            reg_agent reg_process reg_pluginv1
                            info_agent info_process info_ohai]
      }.freeze

      # TODO(al) use the schema to build these lists
      TYPES_WITH_VALUES = ["metric", TYPES[:metric], "heartbeat", TYPES[:heartbeat]].flatten.freeze
      DEFAULT_DAY_BUCKET = ["registration", TYPES[:registration], "info", TYPES[:info]].flatten.freeze

      configure do
        set :show_exceptions, false
        error(500) do
          e = env["sinatra.error"]
          if e.kind_of? Exception
            hastur_error 500, "Server exception: #{e}", e.backtrace
          else
            hastur_error 500, "Server error. Either you found a bug or made a malformed request.", bt
          end
        end
      end

      before do
        if request['Origin']
          response['Access-Control-Allow-Origin'] = "*"
        end
      end

      #
      # @!method /api
      #
      # Top-level resources.
      #
      # @return [Hash{String=>URI}] keys are names, values are resource URIs
      #
      get "/api" do
        json({
          :type     => "#{root_uri}/api/type",
          :app      => "#{root_uri}/api/app",
          :node     => "#{root_uri}/api/node",
        })
      end

      #
      # @!method /api/type
      #
      # A structure of all the supported Hastur message types.
      #
      # @return [Hash{String=>Array<String>}]
      #
      get "/api/type" do
        json TYPES
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

        json h
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

        json array
      end

      #
      # @!method /api/data/node
      #
      # Retrieves a list of currently registered Hastur-enabled nodes
      #
      get "/api/node" do
        h = {}
        start_ts, end_ts = get_start_end :one_day
        uuid_hash = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts

        uuid_hash.keys.each do |uuid|
          uuid_hash[uuid] = "#{root_uri}/api/node/#{uuid}"
        end

        json uuid_hash
      end

      #
      # @!method /api/node/:uuid/:format
      #
      # Retrieve a set of resources for the given UUID. Parameters may be
      # comma-separated values when specifying multiple of a given item.
      #
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param consistency Cassandra read consistency
      #
      get "/api/node/:uuid" do
        start_ts, end_ts = get_start_end :one_day
        uuids = params[:uuid].split(",")

        # reduce the list of UUID's to those that have been seen in the provided time window
        # so invalid UUID's passed in on the URI don't cause bad queries
        seen_uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
        req_uuids = uuids & seen_uuids.keys

        if req_uuids.empty?
          hastur_error 404, "None of #{params[:uuid]} have sent any messages recently."
        else
          array = req_uuids.map do |uuid|
            node = {}
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
          json array
        end
      end

      #
      # @!method /api/node/:uuid/type/:type/:format
      #
      # Retrieve Hastur messages by UUID & type.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param format One of "message", "value", "count" or "rollup" for output format
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra read consistency
      #
      get "/api/node/:uuid/type/:type/:format" do
        query_hastur
      end

      #
      # @!method /api/node/:uuid/name/:name/:format
      #
      # Retrieve Hastur messages by UUID & message name.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param format One of "message", "value", "count" or "rollup" for output format
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra read consistency
      #
      get "/api/node/:uuid/name/:name/:format" do
        query_hastur
      end

      #
      # @!method /api/node/:uuid/type/:type/name/:name/:format
      #
      # Retrieve Hastur messages by UUID, type, and message name.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param format One of "message", "value", "count" or "rollup" for output format
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra read consistency
      #
      get "/api/node/:uuid/type/:type/name/:name/:format" do
        query_hastur
      end

      #
      # @!method /api/node/:uuid/type/:type/:format
      #
      # Retrieve Hastur messages by UUID and type.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param format One of "message", "value", "count" or "rollup" for output format
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param ago How many microseconds back to query - an alternative to start/end
      # @param uuid UUID(s) to query for
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      # @param limit Maximum number of values to return
      # @param reversed Return earliest first instead of latest first
      # @param consistency Cassandra read consistency
      #
      get "/api/node/:uuid/type/:type/:format" do
        query_hastur
      end

      private

      THRIFT_OPTIONS = {
        :timeout => 300,
        :connect_timeout => 30,
        :retries => 10,
      }

      helpers do

        #
        # Get the time range tuple.  Use params or the default period (in seconds).
        #
        # @param [Symbol,String,Fixnum] default delta from current time for start_ts
        # @return Array<Fixnum> start and end epoch usec values
        #
        def get_start_end(default_delta = :five_minutes)
          now = Hastur.timestamp

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

        def param_is_true(name)
          params[name] && !["", "0", "false", "no", "f"].include?(params[name].downcase)
        end

        #
        # Actually query Hastur. The query is based on the Sinatra
        # params. Where appropriate, values can be comma-separated
        # lists.
        #
        # Params can include the following:
        #
        # "format" - the output format - message, value, count or rollup
        # "uuid" - uuid or list of uuids
        # "type" - type or list of types
        # "name" - message name or list of message names (can append * for match-all)
        # "reversed" - return results in reverse order - only matters with "limit"
        # "limit" - max number of results to return
        # "raw" - don't merge messages into the return data, return it as escaped json inside the json
        # "consistency" - Cassandra read consistency
        #
        def query_hastur
          stub! if params["format"] == "rollup"
          unless ["message", "value", "count"].include?(params["format"])
            hastur_error 404, "Illegal output option: '#{params["format"]}'"
          end

          uuids = params["uuid"].split(",")
          types = type_list_from_string(params["type"])
          msg_names = params["name"] ? params["name"].split(",") : []

          unless types.any? { |t| TYPES[:all].include?(t) }
            hastur_error 404, "Invalid type(s): '#{types}'"
          end

          # Some message types are day bucketed and are only expected once a day, like registrations,
          # heartbeats, and ohai information. These should default to getting one day of data.
          if types & DEFAULT_DAY_BUCKET == types
            default_span = :one_day
          else
            default_span = :five_minutes
          end

          start_ts, end_ts = get_start_end default_span

          cass_options = {}
          cass_options[:reversed] = true if param_is_true("reversed")
          cass_options[:value_only] = true if params["format"] == "value"
          cass_options[:count_columns] = true if params["format"] == "count"

          # "count" vs "limit" is an unfortunate naming situation.
          # Cassandra uses "count" to mean "how many results,
          # maximum?"  We use it to mean "please return a count of my
          # results."  We use "limit" for Cassandra's "count".
          # Cassandra uses get_count or count_columns for "please
          # return a count of my results."  I don't think we can win
          # here, Cassandra-naming-wise.
          cass_options[:count] = params["limit"].to_i if params["limit"]

          name_option_list = []
          if msg_names == []
            name_option_list << {}
          else
            msg_names.each do |name|
              if name.include?("*")
                prefix = name.split("*", 2)[0]
                name_option_list << { :name_prefix => prefix }
              else
                name_option_list << { :name => name }
              end
            end
          end

          if params["consistency"]
            cass_options[:consistency] = params["consistency"].to_i
          end

          values = []
          name_option_list.each do |name_opts|
            query_options = cass_options.merge(name_opts)

            @logger.debug("Querying cassandra:", {
              :uuids    => uuids,
              :types    => types,
              :start_ts => start_ts,
              :end_ts   => end_ts,
              :options  => query_options,
            })

            values << Hastur::Cassandra.get(cass_client, uuids, types, start_ts, end_ts, query_options)
          end

          output = {}

          if ["value", "message", "count"].include?(params["format"])
            # Hastur::Cassandra.get returns the following format:
            #   { :uuid => { :type => { :name => { :timestamp => value/object } } } }
            # This REST API returns:
            #   { :uuid => { :name => { :timestamp => value/object } } }

            values.each do |values_for_name_opts|
              values_for_name_opts.each do |uuid, node_data|
                # hash1: {"gauge"=>{"hastur.agent.utime"=>{1338517798448399=>"{\"type\"
                output[uuid] ||= {}
                node_data.each do |type, ts_values|
                  # ts_values maps { :name => { :timestamp => value/object } }
                  # This will return a structure without the types.
                  output[uuid].merge!(ts_values)
                end

                # workaround: we're getting overlaps or unsorted data at this point that causes
                # the series to be jacked up due to out-of-order items, this re-sorts the keys on
                # each row but shouldn't end up doing a lot of copying since it reuses the value reference
                output.each do |uuid, name_vals|
                  name_vals.each do |name, ts_vals|
                    new = {}
                    ts_vals.keys.sort.each do |ts|
                      new[ts] = ts_vals[ts]
                    end
                    output[uuid][name] = new
                  end
                end

                # deserialize the JSON unless the user asks for raw messages so that most use cases
                # only have to deserialize once, so far this doesn't seem to have a speed impact
                if params["format"] == "message" and not param_is_true(:raw)
                  output[uuid].keys.each do |name|
                    output[uuid][name].keys.each do |ts|
                      begin
                        output[uuid][name][ts] = MultiJson.load output[uuid][name][ts]
                      rescue Exception => e
                        hastur_error 501, "JSON parsing failed for: '#{output[uuid][name][ts]}' #{e}"
                      end
                    end
                  end
                end
              end
            end

            add_counts_to(output, values)
          else
            hastur_error 404, "Unhandled output format: '#{params["format"]}'!"
          end

          json output
        end

        def add_counts_to(output, values)
          types = {}
          name_count = 0
          sample_count = 0

          values.each do |values_for_name_opts|
            values_for_name_opts.each do |uuid, node_data|
              types[uuid] ||= {}
              node_data.each do |type, ts_values|
                name_count += ts_values.size
                ts_values.keys.each do |name|
                  types[uuid][name] ||= []
                  types[uuid][name] << type
                end

                sample_count += ts_values.values.map(&:size).inject(&:+)
              end

              types[uuid].each do |name, _|
                types[uuid][name] = types[uuid][name].uniq
              end
            end
          end

          output["uuid_count"] = output.size
          output["name_count"] = name_count
          output["count"] = sample_count
          output["types"] = types
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
        # Dump to JSON string.
        #
        # @param [Hash] content
        # @return [String] Serialized JSON content
        #
        def json(content)
          response['Content-Type'] = "application/json"
          MultiJson.dump(content) + "\n"
        end

        #
        # Calls through to Sinatra's halt with an error code with a JSON body containing
        # {"error": "message"} and the same message in the statusText header.
        #
        def hastur_error(code=501, message="FAIL", bt=nil)
          headers "statusText" => message
          halt(code, json({
              :error => message,
              :url => request.url,
              :backtrace => bt.kind_of?(Array) ? bt[0..10] : bt
            })
          )
        end

        #
        # Ensures that a particular param is present. An HTTP 404 is returned otherwise.
        #
        def check_present(p, human_name = nil)
          unless params[p]
            hastur_error 404, "#{human_name || p} param is required!"
          end
        end

        #
        # Returns an error & status code indicating the method is not implemented yet.
        #
        def stub!(route = "unspecified")
          hastur_error 404, "this route (#{route}) is just a stub and is not implemented yet"
        end

        #
        # Implement the Sinatra forward method so bad requests don't try to pass through
        # to the superclass and return 404 right away.
        #
        def forward
          hastur_error 404, "Invalid path: '#{request.path_info}'"
        end

        #
        # Creates a cassandra client that connects as needed
        # @return [Cassandra] cassandra client object
        #
        def cass_client
          unless @cass_client
            @cass_client = ::Cassandra.new("Hastur", @cassandra_uris.flatten, THRIFT_OPTIONS)

            # for non-production and port-forwarded ssh, there will only be one URI and it
            # should not auto-discover nodes
            if @cassandra_uris.one?
              @cass_client.disable_node_auto_discovery!
            end
          end
          @cass_client
        end
      end

      def initialize(cassandra_uris)
        @logger = Termite::Logger.new
        @cassandra_uris = cassandra_uris
        super
      end
    end
  end
end
