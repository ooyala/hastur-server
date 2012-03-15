#!/usr/bin/env ruby

require "multi_json"
require "cassandra"
require "trollop"
require "hastur"

MultiJson.engine = :yajl

opts = Trollop::options do
  opt :cassandra,    "Cassandra hostname(s)",     :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
  opt :port,         "Port to run server",        :default => 4333,               :type => :integer
end

require "sinatra"
require "hastur-server/sink/cassandra_rollups"

configure do
  set :port, opts[:port]
end

cass_client = Cassandra.new "Hastur", opts[:cassandra].flatten

TYPES = Hastur::Cassandra::SCHEMA.keys

def check_present(param_name, human_name = nil)
  unless params[param_name]
    halt 404, "{ \"msg\": \"#{human_name || param_name} param is required!\" }"
  end
end

before "/data/:type/*" do
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
  uuids.each do |uuid|
    result[uuid] = []
  end

  [ 200, MultiJson.encode(result) ]
end

get "/healthz" do
  # Do a trivial no-op query to see if it 500s
  Hastur::Cassandra.get(cass_client, "nouuid", "stat", 1, 2)

  [ 200, "OK" ]
end

get "/statusz" do
  # Do a trivial no-op query to see if it 500s
  Hastur::Cassandra.get(cass_client, "nouuid", "stat", 1, 2)

  [ 200, "OK" ]
end
