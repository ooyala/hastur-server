#!/usr/bin/env ruby

require "trollop"
require "hastur/sink/cassandra_schema"
require "time"

opts = Trollop.options do
  opt :n,       "Number to insert",    :default => 1,                     :type => :int
  opt :types,   "Types to insert",     :default => ["gauge"],             :type => :strings
  opt :names,   "Names to insert",     :default => ["test-stat"],         :type => :strings
  opt :time,    "Time to mark as",                                        :type => String
  opt :end_time,"End time to mark as",                                    :type => String
  opt :tags,    "Tags to mark as",     :default => ["tag1"],              :type => :strings
  opt :print,   "Print messages",      :default => false,                 :type => :boolean
  opt :insert,  "Write messages to C*",:default => true,                  :type => :boolean
  opt :keyspace,"Keyspace to write",   :default => "Hastur",              :type => String
  opt :host,    "Cassandra host",      :default => "127.0.0.1:9160",      :type => String
end

opts[:time] = Time.now.to_s unless opts[:time]
opts[:end_time] = (Time.now + 60 * 60).to_s unless opts[:end_time]

start_time = Time.parse(opts[:time]).to_i
end_time = Time.parse(opts[:end_time]).to_i

if opts[:n] == 1
  time_increment = 0
else
  time_increment = ((start_time - end_time).to_f / (opts[:n] - 1)).to_i
end

time_start = (start_time.to_f * 1_000_000).to_i

client = Cassandra.new(opts[:keyspace], opts[:host])

opts[:n].times do |i|
  type = opts[:types].sample
  name = opts[:names].sample
  value = rand() * rand() * 1000.0
  time = time_start + i * time_increment
  message = <<EOM
{
  "uuid": "a6-a6-a6-a6-a6-a6-a6",
  "type": "#{type}",
  "name": "#{name}",
  "value": #{value},
  "timestamp": #{time},
  "tags": {
#{opts[:tags].map { |s| "    \"#{s}\": 1" }.join(",\n")}
  }
}
EOM

  puts "Generated stat:\n#{message}" if opts[:print]
  Hastur::Cassandra.insert_stat(client, message) if opts[:insert]
end
