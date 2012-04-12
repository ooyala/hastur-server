$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")

require "sinatra/base"

require "cassandra"
require "hastur"
require "hastur-server/monkeypatch"
require "hastur-server/sink/cassandra_rollups"
require "multi_json"

module Hastur
  class RetrievalApp < Sinatra::Base

    def initialize(cassandra_uris)
      @cassandra_uris = cassandra_uris
      super
    end

    before "/data/:type/*" do
      TYPES = Hastur::Cassandra::SCHEMA.keys
      if params[:type]
        params[:type] = params[:type].downcase
        unless TYPES.include?(params[:type])
          halt 404, <<EOJSON
            { "msg": "Type must be one of: #{TYPES.join ', '}" }
EOJSON
        end
      end   
    end

    #
    # This route returns JSON message objects for the specified type at the
    # given timestamps.
    #
    # Data is returned in a JSON object of the form:
    #   {
    #     "name" => { "ts1" => json, "ts2" => json2, ... },
    #     "name2" => { "ts5" => json5, "ts6" => json6}
    #   }
    #
    # The hash is serialized as JSON which means that each internal JSON
    # chunk must be individually deserialized as well.
    #
    get "/data/:type/json" do
      [ :start, :end, :uuid ].each { |p| check_present p }

      start_ts = ::Hastur.timestamp(params[:start].to_i)
      end_ts = ::Hastur.timestamp(params[:end].to_i)

      # Get with no subtype gives JSON
      values = ::Hastur::Cassandra.get(get_cass_client, params[:uuid], params[:type], start_ts, end_ts)

      # TODO(noah): speed this up by joining, not MultiJson-ing
      values.each do |key, val|
        if val.is_a? ::Hash
          val.each do |ts, json|
            val[ts] = ::MultiJson.decode(json)
          end
        else
          values[key] = ::MultiJson.decode(val)
        end
      end

      [ 200, ::MultiJson.encode(values) ]
    end

    #
    # This route returns values for the given type at the given
    # timestamps.
    #
    # Data is returned in a JSON object of the form:
    #   {
    #     "name" => { "ts1" => value, "ts2" => value2, ... },
    #     "name2" => { "ts5" => value5, "ts6" => value6}
    #   }
    #
    get "/data/:type/values" do
      [ :start, :end, :uuid ].each { |p| check_present p }

      unless [ "stat", "heartbeat" ].include?(params[:type])
        halt 404, <<EOJSON
          { "msg": "Can only get values for types: stat, heartbeat" }
EOJSON
      end

      subtype_list = []
      if params[:type] != "stat" && !params[:subtype]
        subtype_list = [ "" ]  # Subtype is harmless when unsupported
      elsif params[:subtype] && params[:type] == "stat"
        unless [ "gauge", "counter", "mark" ].include?(params[:subtype])
          halt 404, <<EOJSON
            { "msg": "Subtype must be one of: gauge, counter, mark" }
EOJSON
        end
        subtype_list = [ params[:subtype].to_sym ]
      elsif params[:subtype]
        halt 404, <<EOJSON
          { "msg": "Subtype is only for stats" }
EOJSON
      else
        subtype_list = [ :gauge, :counter, :mark ]
      end

      start_ts = ::Hastur.timestamp(params[:start].to_i)
      end_ts = ::Hastur.timestamp(params[:end].to_i)

      values = {}
      subtype_list.each do |subtype|
        value = ::Hastur::Cassandra.get(get_cass_client, params[:uuid], params[:type],
                                      start_ts, end_ts, :subtype => subtype)
        values.merge!(value)
      end

      [ 200, ::MultiJson.encode(values) ]
    end

    get "/data/:type/rollups" do
      [ :start, :end, :uuid, :granularity ].each { |p| check_present p }
    end

    get "/uuids" do
      [ :start, :end ].each { |p| check_present p }

      start_ts = ::Hastur.timestamp(params[:start].to_i)
      end_ts = ::Hastur.timestamp(params[:end].to_i)

      q = ::Hastur::Cassandra.get_uuid_cass_queries_over_time(start_ts, end_ts)
      data = ::Hastur::Cassandra.cass_queries_to_data(get_cass_client, q, :consistency => 1, :count => 10_000)

      [ 200, ::MultiJson.encode(data.values.map(&:keys).flatten) ]
    end

    get "/names/:type" do
      [ :start, :end ].each { |p| check_present p }

      [ 200, "" ]
    end

    #
    # This route returns whether the server is healthy.  A 200 or 500
    # is returned via HTTP.
    #
    get "/healthz" do
      # Do a trivial no-op query to see if it 500s
      ::Hastur::Cassandra.get(get_cass_client, "nouuid", "stat", 1, 2)

      [ 200, "OK" ]
    end

    #
    # This route returns miscellaneous status information.  A 200 or 500
    # is returned via HTTP, along with whatever other information the
    # server feels like sending.
    #
    get "/statusz" do
      # Do a trivial no-op query to see if it 500s
      ::Hastur::Cassandra.get(get_cass_client, "nouuid", "stat", 1, 2)

      [ 200, "OK" ]
    end

    #
    # This route gets one or more registered hostnames for a given
    # UUID.  Those hostnames may come from DNS, from registrations
    # or elsewhere.
    #
    # params[:uuid] must be provided and must either be a UUID or
    # a comma-separated list of UUIDs.
    #
    # The route returns a hash mapping one or more UUIDs to their
    # associated hostname(s) as an array.
    #
    # Example: { UUID1 => [ "foo1.ooyala.com" ] }
    #
    get "/hostnames_for/" do
      [ :uuid ].each { |p| check_present p }
      
      if params[:uuid][","]
        uuids = params[:uuid].split(",").map { |s| s.gsub(/-|_/, "") }.map(&:downcase)
      else
        uuids = [ params[:uuid] ]
      end
      
      result = {}
      registrations = get_registrations
      uuids.each do |uuid|
        reg = registrations[uuid]
        value = nil
        if reg
          value = reg[:hostname]
        end
        result[uuid] = value
      end

      [ 200, ::MultiJson.encode(result) ]
    end

    #
    # This route returns all currently-registered UUIDs as a JSON-encoded
    # array of strings.
    #
    get "/uuids/" do
      uuids = get_registrations.keys
      [ 200, ::MultiJson.encode(uuids) ]
    end

    helpers do
      #
      # This method grabs the most recent registrations from Cassandra and
      # returns them as a hash of hashes:
      #
      # { UUID => reg_hash, UUID2 => reg_hash2, UUID3 => reg_hash3 }
      #
      # Normally the filter parameter will be used to restrict which type(s)
      # of registrations are returned.
      #
      # @return [Hash] The latest registrations per agent UUID
      #
      def get_last_registrations
        last_registrations = {}
        # TODO(noah): Encapsulate this properly in cassanda_schema.rb
        STDERR.puts "Querying Cassandra..."
        ::CASS_CLIENT.each(:RegAgentArchive) do |row, columns|
          uuid = row[0..35]
          last = last_registrations[uuid]
          last_timestamp = last[:timestamp] if last
          last_value = last[:value] if last
          columns.each do |col_key, value|
            next if col_key == "last_access" || col_key == "last_write"
            timestamp = col_key[-8..-1].unpack("Q>")[0]
            if !last_timestamp || timestamp > last_timestamp
              hash = ::MultiJson.decode(value)
              last_timestamp = timestamp
              last_value = hash
            end
          end
          last_registrations[uuid] = { :timestamp => last_timestamp, :json => last_value }
        end
        STDERR.puts "Finished rows"
      end
      
      def check_present(param_name, human_name = nil)
        unless params[param_name]
          halt 404, "{ \"msg\": \"#{human_name || param_name} param is required!\" }"
        end
      end

      def get_registrations
        @last_registration_update ||= 0
        # periodically update registrations
        if ::Time.now - @last_registration_update > 5*60 || @registrations == nil
          @registrations = get_last_registrations
          @last_registration_update = ::Time.now
        end

        @registrations
      end

      def get_cass_client
        unless @cass_client
          @cass_client = ::Cassandra.new("Hastur", @cassandra_uris.flatten)
        end

        @cass_client
      end
    end
  end
end
