require "sinatra/base"

require "cassandra"
require "cgi"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "multi_json"

# TODO(noah): Override for JRuby
MultiJson.use :yajl

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
    #   * consider JSONP callbacks - github has great examples
    #
    class Retrieval < Sinatra::Base
      include Hastur::TimeUtil # import all the usec_* methods

      #
      # All of the Hastur message types. These are used in various places in the API
      # usually in the :type field. The keys may be used to indicate that you want all
      # of the values, so for example, "stat" will get you all counters, gauges, and marks.
      #
      TYPES = {
        :stat         => %w[counter gauge mark],
        :heartbeat    => %w[hb_process hb_agent hb_pluginv1],
        :event        => %w[event],
        :log          => %w[log],
        :error        => %w[error],
        :registration => %w[reg_agent reg_process reg_pluginv1],
        :info         => %w[info_agent info_process],
        :all          => %w[counter gauge mark hb_process hb_agent hb_pluginv1
                            event log error reg_agent reg_process reg_pluginv1
                            info_agent info_process]
      }.freeze

      before "" do
        if request['Origin']
          response['Access-Control-Allow-Origin'] = "*"
        end

        response['Content-Type'] = "application/json"
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
          :node => "#{root_uri}/api/node",
          :app  => "#{root_uri}/api/app",
          :type => "#{root_uri}/api/type",
          :name => "#{root_uri}/api/name",
          :data => "#{root_uri}/api/data",
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
      # @!method /api/node
      #
      # Retrieves a list of currently registered Hastur-enabled nodes
      #
      get "/api/node" do
        h = {}
        get_registrations.each do |uuid, reg_hash|
          h[uuid] = {
            :registration_data => "#{root_uri}/api/node/#{uuid}",
            :message_data => "#{root_uri}/api/data/node/#{uuid}",
          }
        end

        json h
      end

      #
      # @!method /api/node/:uuid
      #
      # Retrieves meta-data on a particular node
      #
      # @param uuid UUID to query for (required)
      #
      get "/api/node/:uuid" do
        # TODO(noah): allow list of UUIDs
        if get_registrations[params[:uuid]]
          registration_hash = get_registrations[params[:uuid]]
          h = {
                :hostname => registration_hash["json"]["hostname"],  # TODO(noah): Update?
                :ipv4     => registration_hash["json"]["ipv4"],
                :data     => "#{root_uri}/api/data/node/#{params[:uuid]}/data",
                :ohai     => "#{root_uri}/api/node/#{params[:uuid]}/ohai",
              }
        else
          error 404, "#{params[:uuid]} is not registered."
        end

        json h
      end

      #
      # @!method /api/node/:uuid/ohai
      #
      # Retrieve Ohai system information.
      # See: http://wiki.opscode.com/display/chef/Ohai
      #
      # @param uuid UUID to query for (required)
      #
      get "/api/node/:uuid/ohai" do
        # TODO(noah): allow list of UUIDs
        start_ts, end_ts = get_start_end :day
        data = Hastur::Cassandra.get(cass_client, params[:uuid], "info_ohai", start_ts, end_ts, :count => 1)

        # reserialize so the json options can be applied
        json MultiJson.load(data[params[:uuid]]["info_ohai"][nil].values.first)
      end

      #
      # @!method /api/app
      #
      # Retrieves all of the registered applications.
      #
      get "/api/app" do
        h = {}
        apps = Set.new
        # Retrieve all registered processes
        cass_client.each(:RegProcessArchive) do |r, c|
          if c.is_a? ::Hash
            c.each do |col_key, value|
              apps.add(MultiJson.load(value)["labels"]["app"]) rescue nil
            end
          end
        end

        # Populate the return data object with the appropriate hash values
        app.each do |app|
          h[app] = {
            :message_data => "#{root_uri}/api/data/app/#{CGI.escape(app)}",
          }
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
      # @example
      #   GET /app/:app/data/
      #   {
      #     "data"    => "/api/app/:app/data/data/"
      #     "gauge"   => "/api/app/:app/data/gauge/"
      #     "counter" => "/api/app/:app/data/counter/"
      #     "event"   => "/api/app/:app/data/event/"
      #     ...
      #   }
      #
      get "/api/app/:app" do
        start_ts, end_ts = get_start_end :one_day
        uuids = Hastur::Cassandra.lookup_by_key cass_client, :app_name, start_ts, end_ts

        h = {
          :app             => CGI.unescape(params[:app]),
          :nodes           => uuids,
          :message_names   => "#{root_uri}/api/app/#{CGI.escape(params[:app])}/name",
          :message_data    => "#{root_uri}/api/data/app/#{CGI.escape(params[:app])}",
        }

        json h
      end

      #
      # @!method /api/app/:app/name
      #
      # Retrieves a list of message names for a particular application
      #
      # @param app URL-encoded application name (required)
      #
      get "/api/app/:app/name" do
        start_ts, end_ts = get_start_end :one_day
        uuids = Hastur::Cassandra.lookup_by_key cass_client, :app_name, start_ts, end_ts

        # TODO(noah): correct for schema.rb's output format
        h = {}
        Hastur::Cassandra.current_schemas.keys.each do |type|
          data = Hastur::Cassandra.get(cass_client, uuids, type, start_ts, end_ts, :consistency => 1)
          data.each do |k, v|
            h[k] = {
              :message_data => "#{root_uri}/api/data/app/#{CGI.escape(params[:app])}/type/#{type}/name/#{k}"
            } unless k.nil?
          end
        end

        json h
      end

      #
      # @!method /api/name
      #
      # Get a list of name resources that have been seen in the last 24-48 hours,
      # with a list of what UUIDs it has been seen on.
      #
      # @return [Hash{String=>URI}]
      #
      get "/api/name" do
        start_ts, end_ts = get_start_end :day
        names = Hastur::Cassandra.lookup_by_key cass_client, :name, start_ts, end_ts

        # TODO: more services for a given name
        # TODO: fix for output format

        data = {}
        names.each do |name,|
          data[name] = {
            :name => name,
          }
        end

        json data
      end

      #
      # @!method /api/data/message
      #
      # Try to retrieve all Hastur messages, everywhere.  Fail.
      #
      # @param start Starting timestamp, default 5 minutes ago
      # @param end Ending timestamp, default now
      # @param uuid UUID(s) to query for
      # @param app_name Application name(s) to query for - no wildcards
      # @param name Message name(s) to query for - supports wildcards
      # @param type Message type(s) to query for
      #
      get "/api/data/message" do
        stub!
      end

      #
      # @!method /api/data/node/:uuid/type/:type/name/:name/value
      #
      # Retrieves the values of a particular message for a particular node
      #
      # @param uuid        UUID to query for (required)
      # @param start       Starting timestamp, default 5 minutes ago
      # @param end         Ending timestamp, default now
      # @param name        Name of the message to query for (required)
      # @param type        Type of message (required)
      # @param reversed    Return results in reverse order - only matters with "limit"
      # @param limit       Maximum number of results to return (default 10,000)
      # @param consistency Cassandra read consistency (default 1)
      #
      get "/api/data/node/:uuid/type/:type/name/:name/value" do
        start_ts, end_ts = get_start_end :five_minutes

        # query cassandra for the data
        opts = { :name => params[:name], :value_only => true }
        values = ::Hastur::Cassandra.get(cass_client, params[:uuid], params[:type], start_ts, end_ts, opts)

        # transform the data into an understandable format
        data = Hash.new
        values.each do |key, val|
          data.merge!(val) if val.is_a? ::Hash
        end

        h = {
              :name  => params[:name],
              :count => data.size,
              :type  => params[:type],
              :data  => data
            }

        json h
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
          types.split(",").map { |type| TYPE_MAPPING[type] || type }.flatten.uniq
        end

        def param_is_true(name)
          params[name] && !["", "0", "false", "no", "f"].include?(params[name].downcase)
        end

        #
        # Actually query Hastur.  This accepts options and automatically
        # sees the Sinatra params.  Usually options come from the URI that
        # called the helper and params are extra overrides provided by the
        # user.  Since the helper checks this, the various routes don't have
        # to do so individually.
        #
        # Where appropriate, values can be comma-separated lists.
        #
        # Options can include the following:
        #
        # :value_only - return values, not full JSON structures
        # :uuid - uuid or list of uuids
        # :type - type or list of types
        # :output - :message, :value, :count or :rollup
        #
        # Params are overridden by options where appropriate.
        # They can include the following:
        #
        # "uuid" - uuid or list of uuids
        # "type" - type or list of types
        # "reversed" - return results in reverse order - only matters with "limit"
        # "limit" - max number of results to return
        # "consistency" - Cassandra read consistency
        #
        # TODO: add app_names, message names.
        #
        def query_hastur(options)
          uuids = (options["uuid"] || params["uuid"]).split(",")
          types = options[:type] || type_list_from_string(params["type"])

          cass_options = {}
          cass_options[:reversed] = true if param_is_true("reversed")
          cass_options[:value_only] = true if options[:value_only]

          # "count" vs "limit" is an unfortunate naming situation.
          # Cassandra uses "count" to mean "how many results,
          # maximum?"  We use it to mean "please return a count of my
          # results."  We use "limit" for Cassandra's "count".
          # Cassandra uses get_count or count_columns for "please
          # return a count of my results."  I don't think we can win
          # here, Cassandra-naming-wise.
          cass_options[:count] = params["limit"].to_i if params["limit"]

          if params["consistency"]
            cass_options[:consistency] = params["consistency"].to_i
          end

          values = Hastur::Cassandra.get(cass_client, uuids, types, start_ts, end_ts, options)

          output = {}
          values.each do |uuid, hash1|
            output[uuid] = {}
            hash1.each do |type, hash2|
              # hash2 is a mapping of { name => { timestamp => value/object } }
              # This will return a structure without the names.
              output[uuid][type] = hash2.values.inject({}, &:merge)
            end
          end

          output
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
        # Retrieves a list of registered agents. Periodically refreshes the registrations
        # depending on how long ago the last refresh was.
        #
        # Defaults to refreshing every five minutes.
        #
        def get_registrations
          @last_registration_update ||= 0
          # periodically update registrations
          if Time.now.to_i - @last_registration_update > 5*60 || @registrations == nil
            @registrations = get_last_agent_registrations
            @last_registration_update = ::Time.now.to_i
          end
          @registrations
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

        #
        # Dump JSON to string with appropriate params.
        # @return [String] Serialized JSON content
        #
        def json(content)
          json_options = {}

          if params[:pretty] or not request.xhr?
            json_options[:pretty] = true
          end

          MultiJson.dump content, json_options
        end

        #
        # Calls through to Sinatra's halt with an error code with a JSON body containing
        # {"error": "message"} and the same message in the statusText header.
        #
        def error(code, message)
          headers "statusText" => message
          halt code, "{\"error\":\"#{message}\"}"
        end

        #
        # Ensures that a particular param is present. An HTTP 404 is returned otherwise.
        #
        def check_present(p, human_name = nil)
          unless params[p]
            error 404, "#{human_name || p} param is required!"
          end
        end

        #
        # Returns an error & status code indicating the method is not implemented yet.
        #
        def stub!(route = "unspecified")
          error 405, "this route (#{route}) is just a stub and is not implemented yet"
        end

        #
        # Grabs the most recent registartions from Cassandra and returns them as
        # a hash of hashes:
        #
        #     {uuid => reg_hash, uuid2 => reg_hash2, ...}
        #
        # Normally the filter parameter will be used to restrict which type(s)
        # of registrations are returned.
        #
        # @todo Move this into schema.rb and/or replace it with something less horrible.
        # @todo Use some kind of real registration rollups instead of querying every reg!
        #
        # @param [Hash] filter The fuzzy_filter hash to restrict registrations returned
        # @param [Hash] The lastest registrations per agent uuid
        #
        def get_last_agent_registrations
          last_registrations = {}
          cass_client.each(:RegAgentArchive) do |r, c|
            uuid = r[0..35]
            last = last_registrations[uuid]
            last_timestamp = last[:timestamp] if last
            last_value = last[:value] if last
            c.each do |col_key, value|
              next if col_key == "last_access" || col_key == "last_write"
              timestamp = col_key[-8..-1].unpack("Q>")[0]
              if !last_timestamp || timestamp > last_timestamp
                hash = MultiJson.decode(value)
                last_timestamp = timestamp
                last_value = hash
              end
            end
            last_registrations[uuid] = { "timestamp" => last_timestamp, "json" => last_value } if last_value
          end
          last_registrations
        end

      end

      def initialize(cassandra_uris)
        @cassandra_uris = cassandra_uris
        super
      end
    end
  end
end
