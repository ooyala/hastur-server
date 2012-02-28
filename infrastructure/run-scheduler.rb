require "hastur-heap-scheduler"

# TODO(viet): For now, test the scheduler with just a flat file
TEMP_JOB_FILE=File.join(File.dirname(__FILE__), "..", "test", "data", "plugin.json")

scheduler = Hastur::Scheduler.new
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
