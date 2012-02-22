#!/usr/bin/env ruby

require "trollop"

# Fake UUID for stat insertion
FAKE_UUID = "fafafafa-fafa-fafa-fafa-fafafafafafa"

opts = Trollop.options do
  opt :host,     "Cassandra hostname",              :type => String,     :default => "127.0.0.1:9160"
  opt :date,     "Date to query",                   :type => String
  opt :uuid,     "UUID of client process to query", :type => String,     :default => FAKE_UUID
  opt :type,     "Type of stat: counter or gauge",  :type => String,     :default => "gauge"
end

require "sinatra"
require "hastur/sink/cassandra_schema"

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

get "/data" do
  start_time = (params[:start].to_f * 1000.0).to_i
  end_time = (params[:end].to_f * 1000.0).to_i

  query = Hastur::Cassandra.get_all_stats(Client, opts[:uuid], start_time, end_time,
                                           :type => opts[:type].to_sym)

  @graph_data = []
  series_num = 1
  prev_time = nil

  query.each do |stat, values|
    data = [ stat ]
    series_num += 1

    values.each do |time, value|
      # Flot timestamps are in milliseconds, not microseconds
      time = time.to_i / 1000

      # Uniquify timestamp
      time = time + 1 if prev_time == time

      data << [ time, value.to_f ]

      prev_time = time
    end
    @graph_data << data
  end

  erb :graph_data
end
