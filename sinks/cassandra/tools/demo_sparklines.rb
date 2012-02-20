#!/usr/bin/env ruby

require "sinatra"
require "trollop"
require "hastur/sink/cassandra_schema"

opts = Trollop.options do
  opt :host,     "Cassandra hostname",              :type => String,     :default => "127.0.0.1:9160"
end

@client = Cassandra.new("Hastur", opts[:host])

get "/" do
  query = {
    "stat1" => {
      "123" => "271.4",
      "456" => "91.8",
      "789" => "-2.3",
    },
    "stat2" => {
      "123" => "1",
      "456" => "2",
      "789" => "3",
    },
    "stat3" => {
      "123" => "1",
      "456" => "2",
      "789" => "3",
    },
  }
  @graph_values = []

  query.each do |stat, values|
    data = [ stat ]
    values.each do |time, value|
      data << value
    end
    @graph_values << data
  end

  erb :demo_sparklines
end
