#!/usr/bin/env ruby

require "hastur/sink/cassandra_schema"
require "cassandra"
require "trollop"
require "date"

opts = Trollop::options do
  opt :rows,     "List row keys",                         :type => :boolean
  opt :stat,     "List a single stat",                    :type => String
  opt :client,   "Client UUID, if any",                   :type => String
  opt :server,   "Cassandra server",                      :type => String,   :default => "127.0.0.1:9160"
  opt :keyspace, "Cassandra keyspace",                    :type => String,   :default => "Hastur"
  opt :type,     "Stat type: counter, gauge, mark, json", :type => String,   :default => "json"
end

client = Cassandra.new(opts[:keyspace], opts[:server])

if opts[:rows]
  cf = Hastur::Cassandra.column_family_for_stat_type(opts[:type].to_sym)

  client.each_key(cf.to_sym) do |key|
    puts key.inspect
  end
  exit 0
end

unless opts[:client]
  raise "Must supply a client (hex) UUID unless just querying rows!"
end

unless opts[:stat]
  raise "Must supply a stat name unless just querying rows!"
end

# Calculate start and end times in microseconds since the Unix epoch
start_time = DateTime.parse("Jan 1, 2010").to_time.to_f * 1_000_000
end_time = DateTime.parse("Jan 1, 2015").to_time.to_f * 1_000_000
start_time = start_time.to_i
end_time = end_time.to_i

vals = Hastur::Cassandra.get_stat(client, opts[:client], opts[:stat], opts[:type].to_sym,
                                  start_time, end_time)

puts vals.inspect
