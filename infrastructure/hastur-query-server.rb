#!/usr/bin/env ruby

require "multi_json"
require "cassandra"
require "trollop"

MultiJson.engine = :yajl

opts = Trollop::options do
  opt :cassandra,    "Cassandra hostname(s)",     :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
end

require "sinatra"
require "hastur-server/sink/cassandra_rollups"

cass_client = Cassandra.new "Hastur", opts[:cassandra].flatten

ROUTES = Hastur::Cassandra::SCHEMA.keys

def check_present(param_name, human_name = nil)
  unless params[param_name]
    halt 404, "{ \"msg\": \"#{human_name || param_name} param is required!\" }"
  end
end

def check_absent(param_name, human_name = nil)
  if params[param_name]
    halt 502, "{ \"msg\": \"#{human_name || param_name} param is unimplemented!\" }"
  end
end

before "/data/:route/*" do
  if params[:route]
    params[:route] = params[:route].downcase
    unless ROUTES.include?(params[:route])
      halt 404, <<EOJSON
{ "msg": "Route must be one of: #{ROUTES.join ', '}" }
EOJSON
    end
  end
end

#
# This route returns JSON message objects for the specified route at the
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
# TODO(noah): Fix these return types
#
get "/data/:route/json" do
  [ :start, :end, :uuid ].each { |p| check_present p }

  # Get with no subtype gives JSON
  values = Hastur::Cassandra.get(cass_client, params[:uuid], params[:route],
                                 params[:start].to_i, params[:end].to_i)

  [ 200, MultiJson.encode(values) ]
end

#
# This route returns values for the given route at the given
# timestamps.
#
# Data is returned in a JSON object of the form:
#   {
#     "name" => { "ts1" => value, "ts2" => value2, ... },
#     "name2" => { "ts5" => value5, "ts6" => value6}
#   }
#
get "/data/:route/values" do
  [ :start, :end, :uuid ].each { |p| check_present p }

  unless [ "stat", "heartbeat" ].include?(params[:route])
    halt 404, <<EOJSON
{ "msg": "Can only get values for routes: stat, heartbeat" }
EOJSON
  end

  subtype_list = []
  if params[:subtype] && params[:route] == "stat"
    unless [ "gauge", "counter", "mark" ].include?(params[:subtype])
      halt 404, <<EOJSON
{ "msg": "Subtype must be one of: gauge, counter, mark" }
EOJSON
    end
    subtype_list = params[:subtype].to_sym
  else
    subtype_list = [ :gauge, :counter, :mark ]
  end

  values = {}
  subtype_list.each do |subtype|
    values.merge!(Hastur::Cassandra.get(cass_client, params[:uuid], params[:route],
                                        params[:start].to_i, params[:end].to_i))
  end

  [ 200, MultiJson.encode(values) ]
end

get "/data/:route/rollups" do
  [ :start, :end, :uuid, :granularity ].each { |p| check_present p }


end

get "/uuids" do
  [ :start, :end ].each { |p| check_present p }

  [ 200, "" ]
end

get "/names/:route" do
  [ :start, :end ].each { |p| check_present p }

  [ 200, "" ]
end

get "/healthz" do
  # TODO(noah): query Cassandra
end

get "/statusz" do
end
