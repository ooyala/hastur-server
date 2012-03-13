#!/usr/bin/env ruby

require "cassandra"
require "trollop"

require "hastur-server/util"
require "hastur-server/sink/cassandra_schema"
require "hastur-server/sink/cassandra_rollups"

#
# The registration rollup will periodically (daily) perform rollups for the RegistrationArchive column family 
#
# TODO(viet): add proper logging via Termite
#

opts = Trollop::options do
  opt :hosts, "Cassandra Hostname(s)", :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
end

GRANULARITY = Hastur::Cassandra::ONE_DAY

client = Cassandra.new("Hastur", opts[:hosts])

loop do
  # query previous day's rollup
  Hastur::Cassandra.get_previous_rollup( client, uuid, "registration", GRANULARITY )
  # query everything that has happened today
  curr_time = Hastur::Utils.timestamp
  start_ts = Hastur::Cassandra.last_time_for_timestamp( curr_time, GRANULARITY )
  end_ts = Hastur::Cassandra.next_time_for_timestamp( curr_time, GRANULARITY )
  Hastur::Cassandra.get( client, uuid, "registration", start_ts, end_ts )
  # TODO(viet): filter out expired plugins
  
  # TODO(viet): combine previous rollup with today's stuff

  # TODO(viet): write today's rollup to cassandra

  sleep 60*60*24 # perform one day rollups
end


