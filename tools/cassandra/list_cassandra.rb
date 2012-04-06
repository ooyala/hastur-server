#!/usr/bin/env ruby

require "hastur-server/sink/cassandra_schema"
require "cassandra"
require "trollop"
require "date"

opts = Trollop::options do
  opt :rows,     "List row keys",                         :type => :boolean
  opt :stat,     "List a single stat",                    :type => String
  opt :agent,    "Agent UUID, if any",                    :type => String
  opt :server,   "Cassandra server",                      :type => String,   :default => "127.0.0.1:9160"
  opt :keyspace, "Cassandra keyspace",                    :type => String,   :default => "Hastur"
  opt :route,    "Hastur message type",                   :type => String,   :default => "stat"
  opt :date,     "Date to query for",                     :type => String
end

agent = Cassandra.new(opts[:keyspace], opts[:server])

if opts[:rows]
  # Calculate the archive row.  This is a hack because list_cassandra is breaking encapsulation by existing.
  cf = "#{opts[:route].capitalize}Archive".to_sym

  agent.each_key(cf.to_sym) do |key|
    puts key.inspect
  end
  exit 0
end

unless opts[:agent]
  raise "Must supply an agent (hex) UUID unless just querying rows!"
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

puts "Querying agent '#{opts[:agent]}', around time #{end_date}."

if opts[:stat]
  puts "Querying stat #{opts[:stat]}, of type #{opts[:type]}."
  vals = Hastur::Cassandra.get_stat(agent, opts[:agent], opts[:stat], opts[:type].to_sym,
                                    start_time, end_time)
else
  puts "Querying all stats."
  vals = Hastur::Cassandra.get_all_stats(agent, opts[:agent], start_time, end_time,
                                         :type => opts[:type].to_sym)
end

puts "Values:"
puts "------"

vals.each do |stat, hash|
  puts "================="
  puts "Stat: #{stat.inspect}"

  if opts[:type].to_sym == :json
    hash.each do |time, json|
      puts "JSON for timestamp #{time}:"
      puts json
      puts "------"
    end
  else
    hash.each do |time, value|
      puts "T #{time} / V #{value.inspect}"
    end
  end

  puts "================="
end
