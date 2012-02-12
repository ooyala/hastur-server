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

begin
  puts "Trying to create keyspace #{opts[:keyspace]}..."
  db.execute("CREATE KEYSPACE #{opts[:keyspace]} WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy' AND strategy_options:replication_factor=1")
rescue Exception
  raise unless $!.message =~ /unique/
  puts "Keyspace seems to exist...  Continuing."
end

db.execute("USE #{opts[:keyspace]}")

COLUMNFAMILIES = [ :StatsArchive ]
COLUMNFAMILIES.each do |cf|
  begin
    puts "Trying to create columnfamily #{cf}..."
    db.execute("CREATE COLUMNFAMILY #{cf} (id utf8 PRIMARY KEY)")
  rescue Exception
    puts $!.inspect
    raise
  end
end

puts "Done!"
