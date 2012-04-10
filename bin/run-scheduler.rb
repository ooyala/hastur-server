#!/usr/bin/env ruby

require "hastur-server/sink/cassandra_schema"
require "hastur-server/hastur-heap-scheduler"
require "hastur-server/util"
require "trollop"
require "cassandra"

opts = Trollop::options do
  opt :routers, "ZMQ URI for the router", :default => ["tcp://127.0.0.1:8126"], :type => :strings, :multi => true
  opt :hosts, "Cassandra Hostname(s)",    :default => ["127.0.0.1:9160"],       :type => :strings, :multi => true
  opt :keyspace, "Cassandra keyspace",    :default => "Hastur",                 :type => String
end

ctx = ZMQ::Context.new(1)
router_socket = Hastur::Util.connect_socket(ctx, ZMQ::PUSH, opts[:routers].flatten)

scheduler = Hastur::Scheduler.new(router_socket)
scheduler.start

# TODO(viet): Use previous 5-minute rollup
# TODO(viet): Deregister plugins

# scrapes Cassandra for any new jobs
scraper = Thread.new do
  begin
    # TODO(viet): Use Noah's library after the naming service is in place. These cassandra
    #             calls are a work around for not knowing which agent UUIDs are currently
    #             registered.
    STDERR.puts "Scheduler connecting to #{opts[:hosts].flatten}"
    client = Cassandra.new("Hastur", opts[:hosts].flatten)
    loop do
      begin
        jobs = []
        end_time = ::Hastur::Util.timestamp
        start_time = end_time.to_i - 60*5*1_000_000  # 5 minutes before
        uuids = Set.new
        # retrieve all agent UUIDs
        client.each_key(:RegAgentArchive) do |key|
          uuids.add( key[0..35] )
        end

        # fetch all of the jobs since 5 minutes ago
        curr_time = Time.now
        uuids.each do |uuid|
          hash = Hastur::Cassandra.get(client, uuid, "reg_agent", start_time, end_time)
          # for registration, there is not a 'name' as a key into the returned hash from Hastur::Cassandra.get()
          ordered_hash = hash[""]
          ordered_hash.keys.each do |key|
            payload = MultiJson.decode ordered_hash[key]
            if payload["type"] == "plugin"
              jobs << Hastur::Job.new(ordered_hash[key], curr_time, uuid)
            end
          end
        end

        # schedule jobs
        scheduler.add_jobs( jobs )
        # TODO(viet): properly log this
        STDERR.puts "#{jobs.size} more jobs scheduled"

        # wait another 10 seconds
        sleep 10
      rescue Exception => e
        STDERR.puts "Error: #{e.inspect}"
        STDERR.puts e.backtrace
        break
        # TODO(viet): Do proper logging here
      end
    end
  rescue Exception => e
    STDERR.puts e.message
    STDERR.puts e.backtrace
    # TODO(viet): Do proper logging here
  end
end

# don't die
scheduler.schedule_thread.join

STDERR.puts "Exiting the Hastur Scheduler..."
