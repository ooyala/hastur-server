require "algorithms"
require "thread"
require "hastur-server/util"
require "hastur-server/message"

module Hastur

  MAX_TIME = 9102444800000000000    # Time.now.to_f * 10**6 will never exceed this

  #
  # Scheduler will execute things based on priority of a Job.
  # After each job is executed, it is reinserted into the scheduler
  # with the recomputed priority.
  #
  class Scheduler
    attr_accessor :heap, :schedule_thread, :mutex

    public

    #
    # socket should be a ZMQ socket that understands how to route Hastur::Messages
    #
    def initialize(socket)
      @heap = Containers::PriorityQueue.new
      @mutex = Mutex.new
      @socket = socket    # socket to send messages on
    end

    #
    # Starts a thread that will continuously schedule and execute jobs 
    # using a priority queue
    #
    def start
      @schedule_thread = Thread.new do
        begin
          # Continuously loop through the heap for the next scheduled job
          job = nil
          loop do
            unless @heap.empty?
              @mutex.synchronize do
                job = @heap.pop
                execute_n_reschedule(job) unless job.nil?
              end
            end
            sleep 0.1
          end
        rescue Exception => e
          STDERR.puts e.message
          STDERR.puts e.backtrace
        end
      end
    end

    #
    # Adds an array of jobs to the priority queue. The priority
    # is computed based on the time that the job should be executed.
    #
    def add_jobs(jobs)
      @mutex.synchronize do
        _add_jobs jobs
      end
    end

    private

    def _add_jobs(jobs)
      jobs.each do |job|
        priority = ::Hastur::MAX_TIME - job.time_to_execute
        @heap.push(job, priority)
      end
    end

    #
    # Schedules a Hastur::Job for its next run, and sends the plugin information to the router
    # at the scheduled time.
    #
    def execute_n_reschedule(job) 
      raise "Must be of type ::Hastur::Job not #{job.class}" unless job.is_a? ::Hastur::Job
     
      # wait until the time is right
      time_diff = (job.time_to_execute - Hastur::Util.timestamp(Time.now)) / 10.0**6
      
      # compute the next time this job should run
      job.time_to_execute += job.interval * 10**6
      _add_jobs([ job ])
      
      sleep time_diff if time_diff >= 0
      
      # execute
      send_to_router(job.json)
    end

    #
    # Notifies the appropriate client that it should execute a plugin
    #
    def send_to_router(payload)
      opts = Hash.new
      opts[:payload] = payload
      # TODO(viet): Figure out the proper format of a plugin payload
      opts[:to] = MultiJson.decode(payload)["uuid"]
      msg = Hastur::Message::PluginExec.new(opts)
      msg.send(@socket)
    end
  end

  #
  # Definition of a Hastur::Job. Contains the raw payload and the Hastur-time
  # at which the job should be executed
  #
  class Job
    attr_accessor :json, :time_to_execute, :interval

    def initialize(json, time_to_execute)
      @json = json
      @time_to_execute = Hastur::Util.timestamp( time_to_execute )
      @interval = MultiJson.decode(@json)["interval"]
    end
  end
end

