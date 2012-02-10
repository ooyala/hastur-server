#!/usr/bin/env ruby

require "rubygems"
require "cassandra-cql"
require "trollop"

opts = Trollop::options do
  banner "Creates a Cassandra keyspace\n\nOptions:"
  opt :host,     "Hostname", :default => "127.0.0.1", :type => String
  opt :keyspace, "Keyspace", :default => "Hastur",    :type => String
end

puts "Connecting to database at #{opts[:host]}:9160"
db = CassandraCQL::Database.new("#{opts[:host]}:9160")

puts "Creating keyspace #{opts[:keyspace]}..."
db.execute("CREATE KEYSPACE #{opts[:keyspace]} WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor=1")
db.execute("USE #{opts[:keyspace]}")
puts "Done!"
