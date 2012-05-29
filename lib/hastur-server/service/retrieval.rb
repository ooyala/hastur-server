require "sinatra/base"

require "cassandra"
require "cgi"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "multi_json"

# TODO(noah): Add parameter validation - alert on bad params?

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
        :stat         => %w[counter gauge mark compound],
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
            :message_data => "#{root_uri}/api/data/node/#{uuid}/message",
          }
        end

        json h
      end

      #
      # @!method /api/node/:uuid
      #
      # Retrieves meta-data on particular node(s).  Returns an array
      # of hashes with hostname, UUID and other data.
      #
      # @param uuid UUID to query for (required)
      #
      get "/api/node/:uuid" do
        registrations = get_registrations

        uuids = params[:uuid].split(",")
        registrations = uuids.map { |uuid| registrations[uuid] }.compact

        if registrations.empty?
          error 404, "None of #{params[:uuid]} are registered."
        else
          array = registrations.map do |registration_hash|
            {
              :hostname => registration_hash["json"]["hostname"],
              :ipv4     => registration_hash["json"]["ipv4"],
              :data     => "#{root_uri}/api/data/node/#{registration_hash["uuid"]}/message",
              :ohai     => "#{root_uri}/api/node/#{registration_hash["uuid"]}/ohai",
            }
          end

          json array
        end
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
        start_ts, end_ts = get_start_end :day
        uuids = params[:uuid].split(",")

        data = Hastur::Cassandra.get(cass_client, uuids, "info_ohai", start_ts, end_ts, :count => 1)

        array = uuids.map do |uuid|
          # reserialize so the json options can be applied
          MultiJson.load(data[uuid]["info_ohai"][nil].values.first)
        end

        json array
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
          h[app] = {
            :message_data => "#{root_uri}/api/data/app/#{CGI.escape(app)}/message",
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
            :app             => bare_app,
            :nodes           => uuids_by_app_name[bare_app] || [],
            :message_names   => "#{root_uri}/api/app/#{CGI.escape(params[:app])}/name",
            :message_data    => "#{root_uri}/api/data/app/#{CGI.escape(params[:app])}/message",
          }
        end

        json array
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
      # @!method /api/data/:format
      #
      # Try to retrieve all Hastur messages, everywhere.  Fail with status 400.
      # Data requests must specify one or more node UUIDs.
      #
      get "/api/data/:format" do
        error 400, "You must specify one or more node UUIDs to query data!"
      end

      #
      # @!method /api/data/name/:name/:format
      #
      # Try to retrieve too many Hastur messages.  Fail with status 400.
      # Data requests must specify one or more node UUIDs.
      #
      get "/api/data/name/:name/:format" do
        error 400, "You must specify one or more node UUIDs to query data!"
      end

      #
      # @!method /api/data/type/:type/:format
      #
      # Try to retrieve too many Hastur messages.  Fail with status 400.
      # Data requests must specify one or more node UUIDs.
      #
      get "/api/data/type/:type/:format" do
        error 400, "You must specify one or more node UUIDs to query data!"
      end

      #
      # @!method /api/data/name/:name/type/:type/:format
      #
      # Try to retrieve too many Hastur messages.  Fail with status 400.
      # Data requests must specify one or more node UUIDs.
      #
      get "/api/data/name/:name/type/:type/:format" do
        error 400, "You must specify one or more node UUIDs to query data!"
      end

      #
      # @!method /api/data/node/:uuid/:format
      #
      # Retrieve Hastur messages.  Parameters may be
      # comma-separated values when specifying multiple
      # of a given item.
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
      get "/api/data/node/:uuid/:format" do
        query_hastur
      end

      #
      # @!method /api/data/node/:uuid/:format
      #
      # Retrieve Hastur messages.  Parameters may be
      # comma-separated values when specifying multiple
      # of a given item.
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
      get "/api/data/node/:uuid/type/:type/:format" do
        query_hastur
      end

      #
      # @!method /api/data/node/:uuid/:format
      #
      # Retrieve Hastur messages.  Parameters may be
      # comma-separated values when specifying multiple
      # of a given item.
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
      get "/api/data/node/:uuid/name/:name/:format" do
        query_hastur
      end

      #
      # @!method /api/data/node/:uuid/:format
      #
      # Retrieve Hastur messages.  Parameters may be
      # comma-separated values when specifying multiple
      # of a given item.
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
      get "/api/data/node/:uuid/type/:type/name/:name/:format" do
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
          (types || "all").split(",").map { |type| TYPE_MAPPING[type] || type }.flatten.uniq
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
        # "name" - message name or list of message names
        # "reversed" - return results in reverse order - only matters with "limit"
        # "limit" - max number of results to return
        # "consistency" - Cassandra read consistency
        #
        def query_hastur(options)
          stub! if params["output"] == :rollup
          unless ["message", "value", "count"].include?(params["output"])
            raise "Illegal output option #{params["output"]}"
          end

          start_ts, end_ts = get_start_end :five_minutes

          uuids = params["uuid"].split(",")
          types = type_list_from_string(params["type"])
          msg_names = params["name"] ? params["name"].split(",") : []

          raise "Not supporting comma-separated list of message names yet!" unless msg_names.size <= 1

          cass_options = {}
          cass_options[:reversed] = true if param_is_true("reversed")
          cass_options[:value_only] = true if params["output"] == "value"
          cass_options[:count_columns] = true if params["output"] == "count"

          # "count" vs "limit" is an unfortunate naming situation.
          # Cassandra uses "count" to mean "how many results,
          # maximum?"  We use it to mean "please return a count of my
          # results."  We use "limit" for Cassandra's "count".
          # Cassandra uses get_count or count_columns for "please
          # return a count of my results."  I don't think we can win
          # here, Cassandra-naming-wise.
          cass_options[:count] = params["limit"].to_i if params["limit"]

          # TODO: support multiple names/prefixes, probably by returning the
          # whole bucket and postfiltering.
          if msg_names[0].include?("*")
            prefix = msg_names[0].split("*", 2)[0]
            cass_options[:name_prefix] = prefix
          else
            cass_options[:name] = msg_names[0]
          end

          if params["consistency"]
            cass_options[:consistency] = params["consistency"].to_i
          end

          values = Hastur::Cassandra.get(cass_client, uuids, types, start_ts, end_ts, options)

          output = {}

          if ["value", "message", "count"].include?(params["output"])
            # Hastur::Cassandra.get returns the following format:
            #   { :uuid => { :type => { :name => { :timestamp => value/object } } } }
            # This REST API returns:
            #   { :uuid => { :name => { :timestamp => value/object } } }
            values.each do |uuid, hash1|
              output[uuid] = {}
              hash1.each do |type, hash2|
                # This will return a structure without the types.
                output[uuid].merge!(hash2)
              end
            end
          else
            raise "Unhandled output format #{params["output"]}!"
          end

          json output
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
        # Pull the first non-hash value in a deep hash, effectively smooshing it.
        #
        def smoosh(data)
          cur = data
          while cur.respond_to? :keys
            cur = cur[cur.keys.first]
          end
          cur
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
        # Get a Cassandra client
        # @return [Cassandra] cassandra client object
        #
        def cass_client
          Hastur::Service::Retrieval.cass_client
        end

        #
        # Dump JSON to string with appropriate params.
        # @return [String] Serialized JSON content
        #
        def json(content)
          json_options = {}

          unless %w[0 false no].include?(params[:pretty]) or request.xhr?
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
          # TODO(noah): Move this into schema.rb
          cass_client.each(:RegAgentArchive) do |r, c|
            uuid = r[0..35]
            last = last_registrations[uuid]
            last_timestamp = last[:timestamp] if last
            last_value = last[:value] if last
            c.each do |col_key, value|
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

      #
      # Sets the Cassandra client to use.  This is a good way to
      # override for testing.
      #
      # @param client The client to use for Cassandra queries
      #
      def self.cass_client=(client)
        @cass_client = client
      end

      #
      # Creates a cassandra client that connects as needed
      # @return [Cassandra] cassandra client object
      #
      def self.cass_client
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
  end
end
