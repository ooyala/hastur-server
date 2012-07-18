require "sinatra/base"

require "cassandra/1.0"
require "cgi"
require "hastur/api"
require "hastur-server/cassandra/schema"
require "hastur-server/cassandra/rollup"
require "hastur-server/time_util"
require "hastur-server/util"
require "hastur-server/aggregation"
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
      FORMATS = %w[message value count rollup].freeze

      # TODO(al) use the schema to build these lists
      TYPES_WITH_VALUES = ["metric", TYPES[:metric], "heartbeat", TYPES[:heartbeat]].flatten.freeze
      DEFAULT_DAY_BUCKET = ["registration", TYPES[:registration], "info", TYPES[:info]].flatten.freeze
      ROLLUP_PERIODS = %w[five_minutes one_hour one_day]

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
        Hastur.mark 'hastur.rest.uri', request.url

        # grab a timestamp at the beginning of each request to use in repeated calls to get_start_end
        env[:hastur_timestamp] = Hastur.timestamp

        # default to pretty printing for non-XHR requests
        params[:pretty] = true unless request.xhr?
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
          json array
        end
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
        start_ts, end_ts = get_start_end :one_day
        uuids = params[:uuid].split(",")

        # might want to cache some of this if this route gets hammered ...
        cnames = Hastur::Cassandra.lookup_by_key cass_client, :cnames, start_ts, end_ts, :count => 1_000_000
        ohais  = Hastur::Cassandra.get cass_client, uuids, "info_ohai", start_ts, end_ts, :count => 1
        regs   = Hastur::Cassandra.get cass_client, uuids, "reg_agent", start_ts, end_ts, :count => 1

        unless ohais.keys.any? or regs.keys.any?
          hastur_error 404, "None of #{params[:uuid]} have registered recently. Try restarting the agent."
        end

        out = {}
        uuids.each do |uuid|
          sys = { :hostname => nil, :fqdn => nil, :nodename => nil, :cnames => [] }

          # first, try the registration information
          if regs[uuid] and regs[uuid]["reg_agent"]
            reg_ts, reg_json = regs[uuid]["reg_agent"][""].shift
            reg = MultiJson.load reg_json rescue {}

            # we only send the fqdn as hostname right now, need to add uname(2) fields
            # agent currently sends :hostname => Socket.gethostname
            sys[:hostname] = reg["hostname"]
            sys[:nodename] = reg["nodename"]

            # /etc/cnames is an Ooyala standard for setting the system's human-facing name
            if reg["etc_cnames"]
              sys[:cnames] = reg["etc_cnames"]
            end
          end

          # use ohai to fill in additional info, including EC2 info
          if ohais[uuid] and ohais[uuid]["info_ohai"]
            ohai_ts, ohai_json = ohais[uuid]["info_ohai"][""].shift
            ohai = MultiJson.load ohai_json rescue {}

            # ohai's 'hostname' is useless, it uses hostname -s to get it
            sys[:hostname] ||= ohai["fqdn"]
            sys[:fqdn]     ||= ohai["fqdn"]

            if ohai["ec2"]
              # use the EC2 info regardless of what the OS says
              sys[:hostname] = ohai["ec2"]["local_hostname"]
              sys[:fqdn]     = ohai["ec2"]["public_hostname"]
            end
          end

          # hosts can have any number of cnames
          sys.values.each do |name|
            if cnames.has_key? name
              sys[:cnames] << cnames[name]
            end
          end
          # don't sort! etc_cnames values should always come first, alphabetical is useless
          sys[:cnames] = sys[:cnames].uniq

          # provide a simple array of all known network names
          # reverse the flattened list so the cnames come first
          sys[:all] = sys.values.flatten.reverse.uniq

          out[uuid] = sys
        end

        json out
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
        json Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)
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

        json out
      end

      #
      # @!method /api/lookup/name
      #
      # Return all UUIDs and all names for that UUID in the same
      # { uuid => { name => {} } } format as data.
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
        out = { :uuid => {}, :name => {} }

        lookup.keys.each do |key|
          info = parse_name_lookup(key)
          out[info[:uuid]] ||= {}
          out[info[:uuid]][info[:name]] = {}
        end

        if params[:fun]
          out = apply_functions(params[:fun], out)
        end

        json out
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
        json names
      end

      #
      # @!method /api/name/:name/:format
      #
      # Retrieve metrics by stat name.
      #
      # @param format One of "message", "value", "count" or "rollup" for output format
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
      get "/api/name/:name/:format" do
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

        query_hastur
      end

      #
      # @!method /api/node/:uuid/type/:type/:format
      #
      # Retrieve Hastur messages by UUID & type.
      # Parameters may be comma-separated values when specifying multiple of a given item.
      #
      # @param format One of "message", "value", "count" or "rollup" for output format
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
      get "/api/node/:uuid/type/:type/:format" do
        query_hastur
      end

      #
      # @!method /statusz
      #
      # A simple health check that hits Cassandra.
      #
      get "/api/statusz" do
        begin
          # cass_client.ring will fail if no successful queries have run
          cass_client.get "GaugeArchive", " "
          ring = cass_client.ring
          out = ring.map do |r|
            { :start_token => r.start_token, :end_token => r.end_token, :endpoints => r.endpoints }
          end
          params[:pretty] = true
          json out
        rescue Exception => e
          hastur_error 500, "Cassandra is not available.", e.backtrace
        end
      end

      private

      THRIFT_OPTIONS = {
        :timeout => 300,
        :connect_timeout => 30,
        :retries => 10,
      }

      helpers do
        #
        # Get the time range tuple.
        #
        # @param [Hash{String => String}] params
        # @param [Symbol,String,Fixnum] default delta from current time for start_ts
        # @return Array<Fixnum> start and end epoch usec values
        #
        def get_start_end(default_delta = :five_minutes)
          now = env[:hastur_timestamp] || Hastur.timestamp

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

        def param_is_true(value)
          value && !["", "0", "false", "no", "f"].include?(value.downcase)
        end

        #
        # Evaluate HTTP query parameters and build a hash of Cassandra query parameters, then return a list
        # of per message name option hashes based off that. The list is necessary for column range queries
        # that are passed in the options hash to Hastur::Cassandra.get.
        #
        # "count" vs "limit" is an unfortunate naming situation. Cassandra uses "count" to mean "how many
        # results, maximum?"  We use it to mean "please return a count of my results."  We use "limit" for
        # Cassandra's "count". Cassandra uses get_count or count_columns for "please return a count of my
        # results."  I don't think we can win here, Cassandra-naming-wise.
        #
        # @param [Array<String>] want_names
        # @return [Array<Hash{Symbol => Object}>] options hash for Hastur::Cassandra.get
        #
        def build_name_option_list(want_names)
          cass_options = {}
          cass_options[:reversed] = true if param_is_true(params[:reversed])
          cass_options[:count] = params[:limit].to_i if params[:limit]

          case params[:format]
          when "value"  ; cass_options[:value_only] = true
          when "rollup" ; cass_options[:rollup_only] = true
          when "count"  ; cass_options[:count_columns] = true
          end

          if params[:rollup_period] or params[:format] == "rollup"
            unless ROLLUP_PERIODS.include?(params[:rollup_period])
              raise "Invalid or missing rollup period: #{params[:rollup_period].inspect}"
            end
            cass_options[:rollup_period] = params[:rollup_period]
          end

          if want_names.any?
            name_option_list = []

            want_names.each do |name|
              if name.include? '*'
                match = name.split('*')
                if name.start_with?('*')
                  raise "Invalid name search '#{name}'. Suffix matching is not supported."
                else
                  name_option_list << cass_options.merge(:name_prefix => match[0])
                end
              else
                name_option_list << cass_options.merge(:name => name)
              end
            end
            name_option_list
          else
            [ cass_options ]
          end
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
        #
        def query_hastur
          unless FORMATS.include?(params[:format])
            hastur_error 404, "Illegal output option: '#{params[:format]}'"
          end

          types = type_list_from_string(params[:type])
          uuids = uuid_or_hostname_to_uuids params[:uuid].split(',')
          names = params[:name] ? params[:name].split(',') : []

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
          name_option_list = build_name_option_list names

          # query cassandra
          values = Hastur.time "hastur.rest.db.query_time" do
            name_option_list.map do |options|
              Hastur::Cassandra.get(cass_client, uuids, types, start_ts, end_ts, options)
            end
          end

          if FORMATS.include?(params[:format])
            output = sort_series_keys(flatten_rows(values))

            if params[:format] == "message"
              output = deserialize_json_messages(output)
            end
          else
            hastur_error 404, "Unhandled output format: '#{params[:format]}'!"
          end

          # Some queries go directly to a Cassandra range scan, which only matches prefixes
          # so a second pass is required to reduce the data down to only what was requested
          # for infix wildcards.
          if names.select {|n| n.include?('*') }.any?
            filter_out_unwanted_names output, names
          end

          if params[:fun]
            output = apply_functions(params[:fun], output)
          end

          json output
        end

        #
        # Apply Hastur aggregation functions. Sets up control information
        # used by the aggregations in some cases and runs the evaluation,
        # returning the transformed result.
        #
        # @param [String] fun aggregation function string, will be unescaped!
        # @param [Hash] series
        # @return [Hash] series
        #
        def apply_functions(fun, series)
          expr = CGI::unescape(fun)

          # pass values needed for hitting Cassandra in
          control = { :cass_client => cass_client }
          control[:start_ts], control[:end_ts] = get_start_end :one_day

          Hastur::Aggregation.evaluate(expr, series, control)
        end

        #
        # Take a list of nodes, where the names may be UUIDs or network names and
        # return a list of just UUIDs. Hostnames that cannot be resolved are dropped
        # from the list.
        #
        # @param [Array<String>] nodes list of UUIDs and network names
        # @return [Array<String>] uuids list of 36-byte UUIDs
        #
        def uuid_or_hostname_to_uuids(nodes)
          # avoid the Cassandra lookup if all the nodes are already UUIDs
          return nodes unless nodes.reject { |node| Hastur::Util.valid_uuid?(node) }.any?

          # node registration is daily, bucket the lookup on day boundary if unspecified
          day_start_ts, day_end_ts = get_start_end :one_day

          uuid_lookup = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", day_start_ts, day_end_ts)

          nodes.flatten.map do |maybe_uuid|
            if Hastur::Util.valid_uuid?(maybe_uuid)
              maybe_uuid
            else
              uuid_lookup[maybe_uuid]
            end
          end.compact
        end

        #
        # We sometimes fetch multiple rows of data from Cassandra and the data returned
        # by Hastur::Cassandra.get isn't an exact match for the JSON format, so merge the rows
        # and drop the type information.
        #
        # Hastur::Cassandra.get returns the following format:
        #   { :uuid => { :type => { :name => { :timestamp => value/object } } } }
        # This REST API returns:
        #   { :uuid => { :name => { :timestamp => value/object } } }
        #
        # @param [Hash] values Hastur::Cassandra.get formatted hash
        # @return [Hash] Hastur V1 output hash
        #
        def flatten_rows(values)
          output = {}
          values.each do |values_for_name_opts|
            values_for_name_opts.each do |uuid, node_data|
              # hash1: {"gauge"=>{"hastur.agent.utime"=>{1338517798448399=>"{\"type\"
              output[uuid] ||= {}
              node_data.each do |type, ts_values|
                # ts_values maps { :name => { :timestamp => value/object } }
                # This will return a structure without the types.
                output[uuid].merge!(ts_values)
              end
            end
          end

          output
        end

        #
        # Due to the merging of series, there can be overlap and the results are always unsorted. Even though
        # JSON/javascript specify associative arrays as unordered, we try to deliver sorted results anyways.
        # We should probably drop this step and specify the V1 JSON format as unordered, but the expectation
        # has already been set with internal users.
        #
        # I tried both in-place modification and this version and was surprised to find copying to a whole
        # new top-level is measurably faster and as a bonus is a pure function.
        #
        # @param [Hash] Hastur V1 output hash
        # @return [Hash] same format, but with the all the series ordered
        #
        def sort_series_keys(values)
          output = {}
          values.each do |uuid, name_series|
            output[uuid] = {}
            name_series.each do |name, series|
              output[uuid][name] = {}
              series.keys.sort.each do |ts|
                output[uuid][name][ts] = series[ts]
              end
            end
          end

          output
        end

        #
        # deserialize JSON messages in the return hash so the end-user can deserialize in one pass
        #
        # @param [Hash] Hastur V1 output hash
        # @return [Hash] same format, but with the all the series ordered
        #
        def deserialize_json_messages(data)
          output = {}
          data.each do |uuid, name_series|
            output[uuid] = {}
            name_series.each do |name, series|
              output[uuid][name] = {}
              series.each do |ts, value|
                # MultiJson gets really upset if you ask it to decode a ruby Hash that ends up
                # being stringified - TODO(al,2012-06-21) figure out why hashes are appearing in this data
                unless value.kind_of? String
                  @logger.debug "BUG: Got a ruby hash where a JSON string was expected."
                  next
                end

                begin
                  output[uuid][name][ts] = MultiJson.load value
                rescue Exception => e
                  hastur_error 501, "JSON parsing failed for: #{value}: #{e}", e.backtrace
                end
              end
            end
          end
          output
        end

        #
        # Parse a name-type_id-uuid key and return it in 3 parts, handles names
        # with dashes in them by popping off the end after split.
        #
        # @param [String] key - key as stored in Cassandra
        # @return [Hash{:name => String, :type_id => Fixnum, :uuid => String}]
        # @example
        #   key = 'collectd.contextswitch-11-079c8b32-8a95-11e1-a1b9-123138124754'
        #   item = parse_name_lookup(key)
        #   {:name => 'collectd.contextswitch', :type_id => 11, :uuid => '079c8b32-8a95-11e1-a1b9-123138124754'}
        #
        def parse_name_lookup(key)
          # uuid & type_id are fixed format, names are not and may contain dashes,
          # so this has to work back-to-front to avoid breaking on names with dashes
          parts = key.split '-'
          uuid = parts.pop(5).join '-'
          type_id = parts.pop.to_i
          name = parts.join '-'
          { :name => name, :type_id => type_id, :uuid => uuid }
        end

        #
        # Check if a given message name string matches the possibly wildcarded
        # match string. Does not use RE evaluation and is safe to use with query parameters.
        #
        # @param [String] name
        # @param [String] match either exact or wildcard match
        # @return [Boolean] true if matches
        #
        def name_matches?(name, match)
          if match.include? '*'
            parts = match.split '*'
            first = parts.shift

            # if it's a leading *, this works because start_with?("") always returns true
            # and has a length of 0 so the position stays at 0, which is correct
            if name.start_with?(first)
              # check for suffix match right away, accounting for a final * which split doesn't return
              if not match.end_with? '*' and not name.end_with?(parts.pop)
                return false
              end

              # check any internal wildcards
              position = first.length
              parts.each do |p|
                # find the substring starting at the position end of the last match
                found = name.index(p, position)
                if found and found >= position
                  position = found + p.length # end of the matched substr
                else
                  return false
                end
              end
            end
          elsif name == match
            true
          end
        end

        #
        # Modify the output hash in-place, deleting any name keys that don't match what the user requested.
        #
        # @param [Hash] output to be modified
        # @param [Array<String>] list of names
        #
        def filter_out_unwanted_names(output, names)
          names.each do |match|
            output.keys.each do |uuid|
              output[uuid].keys.each do |name|
                unless name_matches?(name, match)
                  output[uuid].delete name
                end
              end
            end
          end
        end

        #
        # Look up message names in the "name-" LookupByKey row. Handles comma-separated lists.
        # @see parse_name_lookup
        #
        # @param [String] match_name the name to look up
        # @param [Fixnum] start_ts
        # @param [Fixnum] end_ts
        # @return [Array<Hash{Symbol => String,Fixnum}>]
        #
        def lookup_name(names, start_ts, end_ts)
          names_out = []
          lookup = Hastur::Cassandra.lookup_by_key(cass_client, "name", start_ts, end_ts)

          names.each do |match_name|
            # this will get slower as we get more names in the db, at which point we should
            # add prefix range querying to lookup_by_key where possible
            lookup.keys.map do |key|
              item = parse_name_lookup(key)

              if name_matches?(item[:name], match_name)
                names_out << item
              end
            end
          end
          names_out
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
          # when the cb parameter is specified, return a JSONP response
          if params[:cb]
            response['Content-Type'] = "text/javascript"
            "#{params[:cb]}(#{MultiJson.dump(content)});\n"
          # otherwise, just make it regular JSON
          else
            response['Content-Type'] = "application/json"
            MultiJson.dump(content, :pretty => params[:pretty]) + "\n"
          end
        end

        #
        # Calls through to Sinatra's halt with an error code with a JSON body containing
        # {"error": "message"} and the same message in the statusText header.
        #
        def hastur_error(code=501, message="FAIL", bt=nil)
          headers "statusText" => message

          @logger.error request.url, :error => message, :url => request.url

          halt(code, json({
              :error => message,
              :url => request.url,
              :backtrace => bt.kind_of?(Array) ? bt[0..10] : bt
          }))
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
