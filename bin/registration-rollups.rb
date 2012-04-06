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
REGISTRATION = "registration"
client = Cassandra.new("Hastur", opts[:hosts].flatten)
curr_time = Hastur::Util.timestamp

#
# Retrieves the list of agent UUIDs
#
def get_agent_uuids(c)
  uuids = Set.new
  c.each_key(:RegistrationArchive) do |key|
    uuids.add( key[0..35] )
  end
  uuids
end

#
# TODO(viet): Filters out expired plugins. Not sure how this is going to look like yet.
# This needs to be done, otherwise a "rollup" will essentially be the cumulative registrations
# from the beginning of time.
#
def filter_registrations(ordered_hash)
  ordered_hash
end

# query everything that has happened today
start_ts = Hastur::Cassandra.last_time_segment_for_timestamp( curr_time, GRANULARITY )
end_ts = Hastur::Cassandra.next_time_segment_for_timestamp( curr_time, GRANULARITY )
uuids = get_agent_uuids(client)    # get the list of all agent UUIDs
uuids.each do |uuid|               # for each agent, calculate the registration rollup
  today = Hastur::Cassandra.get( client, uuid, REGISTRATION, start_ts, end_ts )
  today = today[""]   # no name for registration, this is usually a stat name
  today.keys.each do |key|
    # key = timestamp
    payload = today[key]
    # write today's rollup to cassandra
    Hastur::Cassandra.write_rollup( client, REGISTRATION, curr_time, GRANULARITY, uuid, payload )
  end
end

# query previous day's rollup
yesterday = Hastur::Cassandra.get_previous_rollup( client, REGISTRATION, curr_time, GRANULARITY )
yesterday_filtered = filter_registrations(yesterday)

# write today's rollup to cassandra
Hastur::Cassandra.write_ordered_hash_rollup( client, REGISTRATION, curr_time, GRANULARITY, yesterday_filtered )


