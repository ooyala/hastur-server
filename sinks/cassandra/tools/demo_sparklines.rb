#!/usr/bin/env ruby

require "sinatra"
require "trollop"
require "hastur/sink/cassandra_schema"

# Fake UUID for stat insertion
FAKE_UUID = "a6-a6-a6-a6-a6-a6-a6"

opts = Trollop.options do
  opt :host,     "Cassandra hostname",              :type => String,     :default => "127.0.0.1:9160"
  opt :date,     "Date to query",                   :type => String
  opt :uuid,     "UUID of client process to query", :type => String,     :default => FAKE_UUID
  opt :type,     "Type of stat: counter or gauge",  :type => String,     :default => "gauge"
end

Client = Cassandra.new("Hastur", opts[:host])

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

get "/" do
  query = Hastur::Cassandra.get_all_stats(Client, opts[:uuid], start_time, end_time,
                                          :type => opts[:type].to_sym)

  @graph_values = []
  @num = 1
  prev_time = nil

  query.each do |stat, values|
    data = [ "series#{@num}", stat ]
    @num += 1
    values.each do |time, value|
      # Flot timestamps are in milliseconds, not microseconds
      time = time.to_i / 1000

      # Uniquify timestamp
      time = time + 1 if prev_time == time

      data << [ time, value.to_f ]

      prev_time = time
    end
    @graph_values << data
  end

  erb :demo_sparklines
end