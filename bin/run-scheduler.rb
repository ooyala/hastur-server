#!/usr/bin/env ruby

require "hastur-server/sink/cassandra_schema"
require "hastur-server/hastur-heap-scheduler"
require "hastur-server/util"
require "hastur-server/zmq_utils"
require "trollop"
require "cassandra"

opts = Trollop::options do
  opt :routers, "ZMQ URI for the router", :default => ["tcp://127.0.0.1:8126"], :type => :strings, :multi => true
  opt :hosts, "Cassandra Hostname(s)",    :default => ["127.0.0.1:9160"],            :type => :strings, :multi => true
end

ctx = ZMQ::Context.new(1)
router_socket = Hastur::ZMQUtils.connect_socket(ctx, ZMQ::PUSH, opts[:routers].flatten)

scheduler = Hastur::Scheduler.new(router_socket)
scheduler.start

# TODO(viet): Use previous 5-minute rollup
# TODO(viet): Deregister plugins

# scrapes Cassandra for any new jobs
scraper = Thread.new do
  begin
    # TODO(viet): Use Noah's library after the naming service is in place. These cassandra
    #             calls are a work around for not knowing which client UUIDs are currently
    #             registered.
    client = Cassandra.new("Hastur", opts[:hosts])
    loop do
      begin
        jobs = []
        end_time = ::Hastur::Util.timestamp
        start_time = end_time.to_i - 60*5*1_000_000  # 5 minutes before
        uuids = Set.new
        # retrieve all client UUIDs
        client.each_key(:RegistrationsArchive) do |key|
          uuids.add( key[0..35] )
        end

        # fetch all of the jobs since 5 minutes ago
        curr_time = Time.now
        uuids.each do |uuid|
          ordered_hash = Hastur::Cassandra.get(client, uuid, "registration", start_time, end_time)
          ordered_hash.each do |v|
            v.each do |u|
              if u.respond_to?("each")
                u.each do |timestamp, payload|
                  payload_hash = MultiJson.decode(payload)
                  # only grab registrations that are plugin related
                  if payload_hash["type"] == "plugin"
                    jobs << Hastur::Job.new(payload, curr_time, uuid)
                  end
                end
              end
            end
          end
        end

        # schedule jobs
        scheduler.add_jobs( jobs )
        # TODO(viet): properly log this 
        puts "#{jobs.size} more jobs scheduled" 
        
        # wait another 5 minutes
        sleep 5
      rescue Exception => e
        puts "Error: #{e.inspect}"
        puts e.backtrace
        break
        # TODO(viet): Do proper logging here
      end
    end
  rescue Exception => e
    puts e.message
    puts e.backtrace
    # TODO(viet): Do proper logging here
  end
end

# don't die
scheduler.schedule_thread.join

STDERR.puts "Exiting the Hastur Scheduler..."