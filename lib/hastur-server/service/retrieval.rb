require "sinatra/base"

require "cassandra"
require "cgi"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "multi_json"

module Hastur
  module Service
    #
    # The Hastur Retrieval REST service.
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
      }.freeze

      #
      # @!method /
      #
      # Top-level resources.
      #
      # @return [Hash{String=>URI}] keys are names, values are resource URIs
      #
      get "/" do
        hostname = get_request_url(request)

        MultiJson.dump({
          :node => "#{hostname}/node",
          :app  => "#{hostname}/app",
          :type => "#{hostname}/type",
        })
      end

      #
      # @!method /type
      #
      # A structure of all the supported Hastur message types.
      #
      # @return [Hash{String=>Array<String>}]
      #
      get "/type" do
        MultiJson.dump TYPES
      end

      #
      # @!method /node
      #
      # Retrieves a list of currently registered Hastur-enabled nodes
      #
      get "/node" do
        hostname = get_request_url(request)
        h = {}
        get_registrations.each do |uuid, reg_hash|
          h[uuid] = "#{hostname}/node/#{uuid}"
        end

        MultiJson.dump(h)
      end

      #
      # @!method /node/:uuid
      #
      # Retrieves meta-data on a particular node
      #
      # @param uuid UUID to query for (required)
      #
      get "/node/:uuid" do
        hostname = get_request_url(request)
        if get_registrations[params[:uuid]]
          registration_hash = get_registrations[params[:uuid]]
          h = {
                :hostname => registration_hash["json"]["hostname"],
                :ipv4     => registration_hash["json"]["ipv4"],
                :stat     => "#{hostname}/node/#{params[:uuid]}/stat",
                :fact     => "#{hostname}/node/#{params[:uuid]}/fact"
              }
        else
          error 404, "#{params[:uuid]} is not registered."
        end
        MultiJson.dump(h)
      end

      #
      # @!method /node/:uuid/stat
      #
      # Retrieves a list of available stats on a particular node
      #
      # @param uuid UUID to query for (required)
      #
      get "/node/:uuid/stat" do
        hostname = get_request_url(request)
        start_ts, end_ts = get_start_end :one_day

        # Get with no subtype gives JSON
        h = {}
        Hastur::Cassandra::SCHEMA.keys.each do |type|
          data = Hastur::Cassandra.get(cass_client, params[:uuid], type, start_ts, end_ts, :consistency => 1)
          data.each do |k, v|
            h[k] = "#{hostname}/node/#{params[:uuid]}/stat/#{type}/#{k}" unless k.empty?
          end
        end

        MultiJson.dump(h)
      end

      #
      # @!method /node/:uuid/stat/:stat
      #
      # Retrieves the values of a particular stat for a particular node
      #
      # @param uuid    UUID to query for (required)
      # @param start   Starting timestamp, default 5 minutes ago
      # @param end     Ending timestamp, default now
      # @param stat    Name of the stat to query for (required)
      # @param type    Type of stat (required)
      #
      get "/node/:uuid/stat/:type/:stat" do
        start_ts, end_ts = get_start_end :five_minutes

        # query cassandra for the data
        opts = { :name => params[:stat], :value_only => true }
        values = ::Hastur::Cassandra.get(cass_client, params[:uuid], params[:type], start_ts, end_ts, opts)

        # transform the data into an understandable format
        data = Hash.new
        values.each do |key, val|
          data.merge!(val) if val.is_a? ::Hash
        end

        h = {
              :name  => params[:stat],
              :count => data.size,
              :type  => params[:type],
              :data  => data
            }

        MultiJson.dump(h)
      end

      #
      # @!method /app
      #
      # Retrieves all of the registered applications.
      #
      get "/app" do
        h = {}
        hostname = get_request_url(request)
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
          h[app] = "#{hostname}/app/#{CGI.escape(app)}/data"
        end

        MultiJson.dump(h)
      end

      #
      # @!method /app/:app/data
      #
      # Retrieves meta-data about a specific application name.
      #
      # @param app URL-encoded application name (required)
      #
      # @example
      #   GET /app/:app/data/
      #   {
      #     "stat"    => "/app/:app/data/stat/"
      #     "gauge"   => "/app/:app/data/gauge/"
      #     "counter" => "/app/:app/data/counter/"
      #     "event"   => "/app/:app/data/event/"
      #     ...
      #   }
      #
      get "/app/:app/data" do
        hostname = get_request_url(request)
        uuids = get_uuids_from_app_name(params[:app])
        h = {
          :name            => CGI.unescape(params[:app]),
          :number_of_nodes => uuids.size,
          :stats           => "#{hostname}/app/#{CGI.escape(params[:app])}/stat"
        }

        MultiJson.dump(h)
      end

      #
      # @!method /app/:app/node
      # @note not implmented
      #
      # Returns a list of nodes with the application registered.
      #
      get "/app/:app/node" do
        stub! "/app/:app/node"
      end

      #
      # @!method /app/:app/node/:node
      #
      # Get application information for a particular node.
      #
      get "/app/:app/node/:node" do
        stub! "/app/:app/node/:node"
      end

      #
      # @!method /app/:app/stat
      #
      # Retrieves a list of stat name for a particular application
      #
      # @param app URL-encoded application name (required)
      #
      get "/app/:app/stat" do
        hostname = get_request_url(request)
        uuids = get_uuids_from_app_name(params[:app])
        start_ts, end_ts = get_start_end :one_day

        # Get with no subtype gives JSON
        h = {}
        Hastur::Cassandra::SCHEMA.keys.each do |type|
          data = Hastur::Cassandra.get(cass_client, uuids, type, start_ts, end_ts, :consistency => 1)
          data.each do |k, v|
            h[k] = "#{hostname}/app/#{CGI.escape(params[:app])}/stat/#{type}/#{k}" unless k.empty?
          end
        end

        MultiJson.dump(h)
      end

      #
      # @!method /app/:app_name/stat/:stat
      #
      # Retrieves the values of a particular stat across all apps
      #
      # @param app URL-encoded application name (required)
      # @param start   Starting timestamp, default 5 minutes ago
      # @param end     Ending timestamp, default now
      # @param stat    Name of the stat to query for (required)
      # @param type    Type of stat (required)
      #
      get "/app/:app/stat/:type/:stat" do
        h = {}
        uuids = get_uuids_from_app_name(params[:app])
        start_ts, end_ts = get_start_end :five_minutes

        # query cassandra for the data
        opts = { :name => params[:stat] }
        values = ::Hastur::Cassandra.get(cass_client, uuids, params[:type], start_ts, end_ts, opts)

        # transform the data into an understandable format
        data = Hash.new
        values.each do |key, val|
          if val.is_a? ::Hash
            val.each do |ts, json|
              data[ts] = MultiJson.load(json)["value"]
            end
          end
        end

        h = {
              :name  => params[:stat],
              :count => data.size,
              :type  => params[:type],
              :data  => data
            }

        MultiJson.dump(h)
      end

      #
      # @!method /name
      #
      # Get a list of name resources that have been seen in the last 24-48 hours.
      #
      # @return [Hash{String=>URI}]
      #
      get "/name" do
        dump_lookup_rows("name", "name")
      end

      #
      # @!method /uuid
      #
      # Get a list of UUIDs that have been seen in the last 24-48 hours.
      #
      # @return [Hash{String=>URI}]
      #
      get "/uuid" do
        dump_lookup_rows("uuid", "node")
      end

      private

      THRIFT_OPTIONS = {
        :timeout => 300,
        :connect_timeout => 30,
        :retries => 10,
      }

      helpers do

        #
        # Retrieves a list of node UUIDs that are associated with an application
        #
        def get_uuids_from_app_name(app)
          uuids = Set.new
          # Scan all of the registered apps to find the set of associated node UUIDs
          cass_client.each(:RegProcessArchive) do |r, c|
            if c.is_a? ::Hash
              c.each do |col_key, value|
                begin
                  if MultiJson.load(value)["labels"]["app"] == app
                    uuids.add(r[0..35])
                  end
                rescue Exception
                  nil
                end
              end
            end
          end
          uuids.to_a
        end

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

        #
        # Computes the request url without the path information
        #
        def get_request_url(request)
          request.url[0..(request.url.length - request.path_info.length - 1)]
        end

        #
        # Retrieves a list of registered agents. Periodically refreshes the registrations
        # depending on how long ago the last refresh was.
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
        # Grab 24-48 hours of data from the LookupByKey CF and yield the block, giving it key/value pairs.
        #
        # @param [String] kind of key to look up (currently, "name" or "uuid")
        # @yield [String] key returned from Cassandra
        #
        def lookup_by_key(kind)
          start_ts, end_ts = get_start_end :day

          values = {}
          usec_aligned_chunks(start_ts, end_ts, :day).each do |ts|
            cass_client.get('LookupByKey', "#{kind}-#{ts}").each do |key,value|
              yield key, value
            end
          end
        end

        #
        # Convert LookupByKey rows into JSON.
        #
        # @param [String] kind "name" or "uuid" for now
        # @param [String] path to put in the url, e.g. /node/:uuid for UUIDs
        # @return [Hash{String=>URI}]
        #
        def dump_lookup_rows(kind, path)
          hostname = get_request_url(request)

          data = {}
          lookup_by_key kind do |key,|
            data[key] = "http://#{hostname}/#{path}/#{key}"
          end

          MultiJson.dump data
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
        def stub!
          error 405, "this route is just a stub and is not implemented yet"
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
