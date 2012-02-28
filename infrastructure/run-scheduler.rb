#!/usr/bin/env ruby

require "hastur-heap-scheduler"
require "hastur-server/zmq_utils"
require "trollop"

opts = Trollop::options do
  opt :router, "ZMQ URI for the router", :default => "tcp://127.0.0.1:8126", :type => String
end

if opts[:router] !~ /\w+:\/\/[^:]+:\d+/
  Trollop::die :router, "Option --router must be of the form protocol://hostname:port"
end

if ZMQ::LibZMQ.version2? && opts[:router] =~ /\Wlocalhost\W/
  Trollop::die :router, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
end

# TODO(viet): For now, test the scheduler with just a flat file
TEMP_JOB_FILE=File.join(File.dirname(__FILE__), "..", "test", "data", "plugin.json")

ctx = ZMQ::Context.new(1)
router_socket = Hastur::ZMQUtils.connect_socket(ctx, ZMQ::PUSH, opts[:router])

scheduler = Hastur::Scheduler.new(router_socket)
scheduler.start

# read scheduled jobs
f = File.new(TEMP_JOB_FILE, "r")
jobs = []
curr_time = Time.new
while line = f.gets
  jobs << Hastur::Job.new(line, curr_time)
end

# push all scheduled jobs through the scheduler
scheduler.add_jobs jobs

# don't die
schedule.schedule_thread.join

STDERR.puts "Exiting the Hastur Scheduler..."
