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

STDERR.puts "Connecting to Cassandra: #{opts[:cassandra].flatten.inspect}"
CASS_CLIENT = Cassandra.new "Hastur", opts[:cassandra].flatten

def get_last_registrations
  last_registrations = {}
  # TODO(noah): Encapsulate this properly in cassanda_schema.rb
  STDERR.puts "Querying Cassandra..."
  CASS_CLIENT.each(:RegistrationArchive) do |row, columns|
    STDERR.puts "  - Got row"
    uuid = row[0..35]
    last = last_registrations[uuid]
    last_timestamp = last[:timestamp] if last
    last_value = last[:value] if last

    columns.each do |col_key, value|
      timestamp = col_key[-8..-1].unpack("Q>")[0]
      if !last_timestamp || timestamp > last_timestamp
        last_timestamp = timestamp
        last_value = value
      end
    end

    last_registrations[uuid] = { :timestamp => last_timestamp, :json => last_value }
  end
  STDERR.puts "Finished rows"
end

@initialized = false
t = Thread.new do
  begin
    loop do
      @registrations = get_last_registrations

      STDERR.puts "Initialized! *************************************"
      @initialized = true
      sleep 5 * 60
    end
  rescue Exception
    STDERR.puts "Exception: #{$!.message}"
    STDERR.puts $!.backtrace.join("\n")
  end
end

sleep 0.01 until @initialized

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
    reg = @registrations[uuid]
    value = nil
    if reg
      hash = MultiJson.decode(reg) rescue nil
      if hash
        value = hash[:hostname]
      end
    end

    result[uuid] = value
  end

  [ 200, MultiJson.encode(result) ]
end

get "/healthz" do
  # Do a trivial no-op query to see if it 500s
  Hastur::Cassandra.get(CASS_CLIENT, "nouuid", "stat", 1, 2)

  [ 200, "OK" ]
end

get "/statusz" do
  # Do a trivial no-op query to see if it 500s
  Hastur::Cassandra.get(CASS_CLIENT, "nouuid", "stat", 1, 2)

  [ 200, "OK" ]
end
