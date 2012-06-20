#!/usr/bin/env ruby
#
# Build an hostname -> UUID lookup table in Cassandra for the REST service to use.
#
# Eventually, this will be a Hastur trigger that runs in real-time when registrations
# and Ohai data arrive. When/if we add the git commit feed, we can probably process DNS
# updates as they occur too.
#

require "cassandra/1.0"
require "hastur/api"
require "hastur-server/cassandra/rollups"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "multi_json"
require "termite"
require "trollop"
require "time"

opts = Trollop::options do
  opt :cassandra, "Cassandra server list", :default => ["127.0.0.1:9160"], :type => :strings, :multi => true
  opt :keyspace, "Cassandra Keyspace to use", :default => "Hastur"
end

cass_client = ::Cassandra.new(opts[:keyspace], opts[:cassandra].flatten)
cass_client.disable_node_auto_discovery!

end_ts = Hastur::TimeUtil.usec_epoch
start_ts = end_ts - Hastur::TimeUtil::USEC_TWO_DAYS

puts "Starting on: #{Hastur::TimeUtil.usec_to_time(start_ts).iso8601}"
puts "Ending on: #{Hastur::TimeUtil.usec_to_time(end_ts).iso8601}"

cnames = Hastur::Cassandra.lookup_by_key cass_client, :cnames, start_ts, end_ts
uuids  = Hastur::Cassandra.lookup_by_key cass_client, :uuid, start_ts, end_ts
ohais  = Hastur::Cassandra.get cass_client, uuids.keys, "info_ohai", start_ts, end_ts
regs   = Hastur::Cassandra.get cass_client, uuids.keys, "reg_agent", start_ts, end_ts

# partially borrowed from the retrieval service's uuid -> hostname route
out = {}
uuids.keys.each do |uuid|
  # first, try the registration information
  if regs[uuid] and regs[uuid]["reg_agent"]
    reg_ts, reg_json = regs[uuid]["reg_agent"][""].shift
    reg = MultiJson.load reg_json rescue {}

    out[reg["hostname"]] = uuid
    out[reg["nodename"]] = uuid

    # /etc/cnames is an Ooyala standard for setting the system's human-facing name
    # cnames should pretty much always be fully-qualified
    if reg["etc_cnames"]
      reg["etc_cnames"].each do |cname|
        out[cname] = uuid
      end
    end
  end

  # use ohai to fill in additional info, including EC2 info
  if ohais[uuid] and ohais[uuid]["info_ohai"]
    ohai_ts, ohai_json = ohais[uuid]["info_ohai"][""].shift
    ohai = MultiJson.load ohai_json rescue {}

    # ohai hostname is almost always useless since they call hostname -s
    # fqdn is fine since they call hostname --fqdn
    out[ohai["fqdn"]] = uuid

    # ec2 names are pretty much globally unique, cram them in
    if ohai["ec2"]
      out[ohai["ec2"]["local_hostname"]] = uuid
      out[ohai["ec2"]["public_hostname"]] = uuid
    end
  end

  unless out.values.include? uuid
    STDERR.print "Could not create a single map for uuid #{uuid}!\n"
  end
end

# one more pass to add cnames from DNS zonefiles
out.keys.each do |hostname|
  if cnames.has_key? hostname
    out[cnames[hostname]] = out[hostname]
  end

  out.delete(hostname) if hostname.nil?
  out.delete(hostname) unless hostname =~ /\w+\.\w+/
end

# write into a daily bucket, overwrite is the common case
bucket_ts = Hastur::TimeUtil.usec_truncate end_ts, :one_day

example = { :LookupByKey => { "host-uuid-#{bucket_ts}" => out } }
puts MultiJson.dump(example, :pretty => true)

cass_client.insert(:LookupByKey, "host-uuid-#{bucket_ts}", out)
