require "sinatra/base"

require "cassandra"
require "cgi"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "multi_json"

module Hastur
  module Service
    #
    # The Retrieval application.
    #
    # Message types include:
    #
    #   stat - statistics
    #     counter
    #     gauge
    #     mark
    #   heartbeat - heartbeats from processes or systems
    #     hb_process
    #     hb_agent
    #     hb_pluginv1
    #   event
    #   log
    #   error
    #   registration - registrations from processes or systems
    #     reg_agent
    #     reg_process
    #     reg_pluginv1
    #   info
    #     info_agent
    #     info_process
    #
    class Retrieval < Sinatra::Base
      THRIFT_OPTIONS = {
        :timeout => 300,
        :connect_timeout => 30,
        :retries => 10,
      }

      #
      # @!method /
      #
      # Top-level resources
      #
      get "/" do
        hostname = get_request_url(request)
        h = {:nodes => "#{hostname}/nodes", :apps => "#{hostname}/apps"}

        ::MultiJson.dump(h)
      end

      #
      # @!method /nodes
      #
      # Retrieves a list of currently registered Hastur-enabled nodes
      #
      get "/nodes" do
        hostname = get_request_url(request)
        h = {}
        get_registrations.each do |uuid, reg_hash|
          h[uuid] = "#{hostname}/nodes/#{uuid}"
        end

        ::MultiJson.dump(h)
      end

      #
      # @!method /nodes/:uuid
      #
      # Retrieves meta-data on a particular node
      #
      # @params uuid UUID to query for (required)
      #
      get "/nodes/:uuid" do
        hostname = get_request_url(request)
        if get_registrations[params[:uuid]]
          registration_hash = get_registrations[params[:uuid]]
          h = {
                :hostname => registration_hash["json"]["hostname"],
                :ipv4     => registration_hash["json"]["ipv4"],
                :stats    => "#{hostname}/nodes/#{params[:uuid]}/stats",
                :facts    => "#{hostname}/nodes/#{params[:uuid]}/facts"
              }
        else
          return [404, ::MultiJson.dump( { :msg => "#{params[:uuid]} is not registered." } )]
        end
        ::MultiJson.dump(h)
      end

      #
      # @!method /nodes/:uuid/stats
      #
      # Retrieves a list of available stats on a particular node
      #
      # @params uuid UUID to query for (required)
      #
      get "/nodes/:uuid/stats" do
        hostname = get_request_url(request)
        start_ts, end_ts = get_start_end :one_day

        # Get with no subtype gives JSON
        h = {}
        Hastur::Cassandra::SCHEMA.keys.each do |type|
          data = Hastur::Cassandra.get(get_cass_client, params[:uuid], type, start_ts, end_ts, :consistency => 1)
          data.each do |k, v|
            h[k] = "#{hostname}/nodes/#{params[:uuid]}/stats/#{type}/#{k}" unless k.empty?
          end
        end

        ::MultiJson.dump(h)
      end

      #
      # @!method /nodes/:uuid/stats/:stat
      #
      # Retrieves the values of a particular stat for a particular node
      #
      # @params uuid    UUID to query for (required)
      # @params start   Starting timestamp, default 5 minutes ago
      # @params end     Ending timestamp, default now
      # @params stat    Name of the stat to query for (required)
      # @params type    Type of stat (required)
      #
      get "/nodes/:uuid/stats/:type/:stat" do
        start_ts, end_ts = get_start_end :five_minutes

        # query cassandra for the data
        opts = { :name => params[:stat] }
        values = ::Hastur::Cassandra.get(get_cass_client, params[:uuid], params[:type], start_ts, end_ts, opts)

        # transform the data into an understandable format
        data = Hash.new
        values.each do |key, val|
          if val.is_a? ::Hash
            val.each do |ts, json|
              data[ts] = ::MultiJson.load(json)["value"]
            end
          end
        end

        h = {
              :name  => params[:stat],
              :count => data.size,
              :type  => params[:type],
              :data  => data
            }

        ::MultiJson.dump(h)
      end

      #
      # @!method /apps
      #
      # Retrieves all of the registered applications.
      #
      get "/apps" do
        h = {}
        hostname = get_request_url(request)
        apps = Set.new
        get_cass_client.each(:RegProcessArchive) do |r, c|
          if c.is_a? ::Hash
            c.each do |col_key, value|
              begin
                apps.add(CGI.escape(MultiJson.load(value)["labels"]["app"]))
              rescue Exception => e
                
              end
            end
          end
        end

        apps.each do |app|
          h[app] = "#{hostname}/apps/#{app}"
        end

        ::MultiJson.dump(h)
      end

      #
      # @!method /apps/:app
      #
      # Retrieves meta-data about a specific application name.
      #
      get "/apps/:app" do
        hostname = get_request_url(request)
        h = {
          :name            => CGI.unescape(params[:app]),
#          :number_of_nodes => number_of_nodes,
          :stats           => "#{hostname}/apps/#{CGI.escape(params[:app])}/stats"
        }

        ::MultiJson.dump(h)
      end

      #
      # @!method /apps/:app/stats
      #
      # Retrieves a list of stat name for a particular application
      #
      get "/apps/:app/stats" do
        
      end

      #
      # @!method /apps/:app_name/stats/:stat
      #
      # Retrieves the values of a particular stat across all apps
      #
      # @params uuid    UUID to query for (required)
      # @params start   Starting timestamp, default 5 minutes ago
      # @params end     Ending timestamp, default now
      # @params stat    Name of the stat to query for (required)
      # @params type    Type of stat (required)
      #
      get "/apps/:app/stats/:stat" do
        
      end

      helpers do

        #
        # Turn a string or number into a number of usecs.
        #
        def delta_usec(delta)
          case delta.to_s
            when "one_minute"   ;     60_000_000
            when "five_minutes" ;    600_000_000
            when "one_hour"     ;  3_600_000_000
            when "one_day"      ; 86_400_000_000
            when /\A\d+\Z/      ; delta.to_i
          end
        end

        #
        # Get the time range tuple.  Use params or the default period (in seconds).
        #
        # @param [Symbol,String,Fixnum] default delta from current time for start_ts
        # @return Array<Fixnum> start and end epoch usec values
        #
        def get_start_end(default_delta = "five_minutes")
          if params[:end]
            end_ts = Hastur.timestamp(params[:end].to_i)
          else
            end_ts = Hastur.timestamp
          end

          if params[:start]
            start_ts = Hastur.timestamp(params[:start].to_i)
          elsif params[:ago]
            start_ts = Hastur.timestamp - delta_usec(params[:ago])
          else
            start_ts = end_ts - delta_usec(default_delta)
          end

          return start_ts, end_ts
        end

        #
        # Computes the request url without the path information
        #
        def get_request_url(request)
          request.url[0..(request.url.length - request.path_info.length - 1)]
        end #
        # Retrieves a list of registered agents. Periodically refreshes the registrations
        # depending on how long ago the last refresh was.
        #
        def get_registrations
          @last_registration_update ||= 0
          # periodically update registrations
          if ::Time.now.to_i - @last_registration_update > 5*60 || @registrations == nil
            @registrations = get_last_agent_registrations
            @last_registration_update = ::Time.now.to_i
          end
          @registrations
        end

        #
        # Creates a cassandra client that connects as needed
        #
        def get_cass_client
          @cass_client ||= ::Cassandra.new("Hastur", @cassandra_uris.flatten, THRIFT_OPTIONS)
        end

        #
        # Ensures that a particular param is present. An HTTP 404 is returned otherwise.
        #
        def check_present(p, human_name = nil)
          unless params[p]
            halt [404, "{ \"msg\" : \"#{human_name || p} param is required!\" }"]
          end
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
          get_cass_client.each(:RegAgentArchive) do |r, c|
            uuid = r[0..35]
            last = last_registrations[uuid]
            last_timestamp = last[:timestamp] if last
            last_value = last[:value] if last
            c.each do |col_key, value|
              next if col_key == "last_access" || col_key == "last_write"
              timestamp = col_key[-8..-1].unpack("Q>")[0]
              if !last_timestamp || timestamp > last_timestamp
                hash = ::MultiJson.decode(value)
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
