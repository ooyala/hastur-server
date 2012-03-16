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
require "hastur-server/monkeypatch"

configure do
  set :port, opts[:port]
end

STDERR.puts "Connecting to Cassandra: #{opts[:cassandra].flatten.inspect}"
CASS_CLIENT = Cassandra.new "Hastur", opts[:cassandra].flatten

#
# This method grabs the most recent registrations from Cassandra and
# returns them as a hash of the format:
#
# { UUID => reg_hash, UUID2 => reg_hash2, UUID3 => reg_hash3 }
#
# Normally the filter parameter will be used to restrict which type(s)
# of registrations are returned.
#
# @param [Hash] filter The fuzzy_filter hash to restrict registrations returned
# @return [Hash] The latest registrations per client UUID
#
def get_last_registrations(filter)
  last_registrations = {}
  # TODO(noah): Encapsulate this properly in cassanda_schema.rb
  STDERR.puts "Querying Cassandra..."
  CASS_CLIENT.each(:RegistrationArchive) do |row, columns|
    uuid = row[0..35]
    last = last_registrations[uuid]
    last_timestamp = last[:timestamp] if last
    last_value = last[:value] if last

    columns.each do |col_key, value|
      next if col_key == "last_access" || col_key == "last_write"
      timestamp = col_key[-8..-1].unpack("Q>")[0]
      if !last_timestamp || timestamp > last_timestamp
        hash = MultiJson.decode(value)
        next if [hash].fuzzy_filter(filter) == []

        last_timestamp = timestamp
        last_value = hash
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
      @registrations = get_last_registrations("type" => "client")

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
      value = reg[:hostname]
    end

    result[uuid] = value
  end

  [ 200, MultiJson.encode(result) ]
end

#
# This route returns all currently-registered UUIDs as a JSON-encoded
# array of strings.
#
get "/uuids/" do
  uuids = @registrations.keys
  [ 200, MultiJson.encode(uuids) ]
end

#
# This route returns whether the server is healthy.  A 200 or 500
# is returned via HTTP.
#
get "/healthz" do
  # Do a trivial no-op query to see if it 500s
  Hastur::Cassandra.get(CASS_CLIENT, "nouuid", "stat", 1, 2)

  [ 200, "OK" ]
end

#
# This route returns miscellaneous status information.  A 200 or 500
# is returned via HTTP, along with whatever other information the
# server feels like sending.
#
get "/statusz" do
  # Do a trivial no-op query to see if it 500s
  Hastur::Cassandra.get(CASS_CLIENT, "nouuid", "stat", 1, 2)

  [ 200, "OK" ]
end
