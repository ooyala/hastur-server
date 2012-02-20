#!/usr/bin/env ruby

require "sinatra"
require "trollop"
require "hastur/sink/cassandra_schema"

opts = Trollop.options do
  opt :host,     "Cassandra hostname",              :type => String,     :default => "127.0.0.1:9160"
end

@client = Cassandra.new("Hastur", opts[:host])

get "/" do
  @graph_values = [ 271.4, 91.8, -2.3, 129.7, 7.1 ]
  erb :demo_sparklines
end
