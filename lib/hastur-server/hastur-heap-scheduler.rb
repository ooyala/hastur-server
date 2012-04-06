require "algorithms"
require "digest/sha1"
require "set"
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
    attr_accessor :heap, :schedule_thread, :mutex, :test_mode, :msg_buffer, :job_set

    public

    #
    # socket should be a ZMQ socket that understands how to route Hastur::Messages
    #
    def initialize(socket, test_mode=false)
      @heap = Containers::PriorityQueue.new
      @mutex = Mutex.new
      @socket = socket    # socket to send messages on
      @test_mode = test_mode
      @msg_buffer = []
      @job_set = Set.new
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
                @job_set.delete(Digest::SHA1.hexdigest(job.json))
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

    def stop
      @schedule_thread.kill
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
        sha = Digest::SHA1.hexdigest(job.json)
        # only add the job if it is not already in the heap
        unless @job_set.include?(sha)
          @job_set.add( sha )
          priority = ::Hastur::MAX_TIME - job.time_to_execute
          @heap.push(job, priority)
        end
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
      send_to_router(job.json, job.uuid)
    end

    #
    # Notifies the appropriate agent that it should execute a plugin
    #
    def send_to_router(payload, uuid)
      if @test_mode
        @msg_buffer << payload
      else
        opts = Hash.new
        opts[:payload] = payload
        opts[:to] = uuid
        opts[:from] = "fafafafa-fafa-fafa-fafa-fafafafafafa"    # every Hastur::Messages requires a :from
        msg = Hastur::Message::PluginExec.new(opts)
        msg.send(@socket)
        STDERR.puts "Scheduling plugin for #{uuid} => #{payload}"
      end
    end
  end

  #
  # Definition of a Hastur::Job. Contains the raw payload and the Hastur-time
  # at which the job should be executed
  #
  class Job
    attr_accessor :json,            # payload to send across the wire
                  :time_to_execute, # Hastur time to send request
                  :interval,        # Interval between requests in seconds
                  :uuid             # agent UUID

    def initialize(json, time_to_execute, uuid)
      @uuid = uuid
      @json = json
      @time_to_execute = Hastur::Util.timestamp( time_to_execute )
      interval = MultiJson.decode(@json)["interval"].to_sym
      case interval
      when :five_minutes
        @interval = 5*60
      when :thirty_minutes
        @interval = 30*60
      when :hourly
        @interval = 60*60
      when :daily
        @interval = 60*60*24
      when :monthly
        @interval = 60*60*24*30
      else
        raise "Unable to determine the interval of the plugin: #{interval.inspect}"
      end
    end
  end
end

