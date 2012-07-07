#!/usr/bin/env ruby

require "hastur"
require "cassandra/1.0"
require "cassandra/constants"
require "hastur-server/cassandra/rollup"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "termite"
require "trollop"
require "time"

opts = Trollop::options do
  opt :cassandra, "Cassandra server list", :default => ["127.0.0.1:9202"], :type => :strings, :multi => true
  opt :keyspace, "Cassandra Keyspace to use", :default => "Hastur"
end

cass_client = ::Cassandra.new(opts[:keyspace], opts[:cassandra].flatten)
cass_client.disable_node_auto_discovery!

end_ts = Hastur::TimeUtil.usec_epoch
start_ts = end_ts - Hastur::TimeUtil::USEC_ONE_HOUR

uuids = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts

Hastur.time "hastur.job.rollup.all.gauges" do
  Hastur::Cassandra::Rollup.rollups_for_range cass_client, 'gauge', uuids, start_ts, end_ts
end

Hastur.time "hastur.job.rollup.all.counters" do
  Hastur::Cassandra::Rollup.rollups_for_range cass_client, 'counter', uuids, start_ts, end_ts
end

