#!/opt/hastur/bin/ruby

# pull lookup_by_key rows and dump them to console
# by default, gets all the current lookup rows and prints them in JSON

require "hastur"
require "multi_json"
require "cassandra/1.0"
require "cassandra/constants"
require "hastur-server/cassandra/rollup"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "termite"
require "trollop"
require "time"

include Hastur::TimeUtil

LOOKUPS = %w[uuid name app_name host-uuid cnames]

opts = Trollop::options do
  opt :first,     "first timestamp",  :default => usec_epoch
  opt :last,      "last timestamp",   :default => usec_epoch
  opt :index,     "index to list",    :default => LOOKUPS, :multiple => true
  opt :cassandra, "cassandra server", :default => "127.0.0.1:9160", :multiple => true
end

cass = ::Cassandra.new("hastur", [opts[:cassandra]].flatten)
cass.disable_node_auto_discovery!

day_chunks = usec_aligned_chunks(opts[:first], opts[:last], :day)

# all uuids in the time range
out = {}
opts[:index].flatten.each do |lookup|
  day_chunks.each do |ts|
    cass.get('lookup_by_key', "#{lookup}-#{ts}").each do |key,value|
      puts MultiJson.dump [key, value]
    end
  end
end

