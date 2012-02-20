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
  opt :date,     "Date to query for",                     :type => String
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

if opts[:date]
  end_date = Date.parse(opts[:date]).to_time.utc + 24 * 60 * 60 - 1
else
  end_date = Time.now.utc
end

# Calculate start and end times in microseconds since the Unix epoch
start_time = (end_date - 23 * 60 * 60).to_time.to_f * 1_000_000
end_time = end_date.to_time.to_f * 1_000_000
start_time = start_time.to_i
end_time = end_time.to_i

puts "Querying client '#{opts[:client]}', around time #{end_date}."

if opts[:stat]
  puts "Querying stat #{opts[:stat]}, of type #{opts[:type]}."
  vals = Hastur::Cassandra.get_stat(client, opts[:client], opts[:stat], opts[:type].to_sym,
                                    start_time, end_time)
else
  puts "Querying all stats."
  vals = Hastur::Cassandra.get_all_stats(client, opts[:client], start_time, end_time)
end

puts "===================================="
puts "Values:"
puts "------"
puts vals.inspect
puts "===================================="
