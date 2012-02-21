require "multi_json"
require "socket"

#
# Hastur API gem that allows services/apps to easily publish
# correct Hastur-commands to their local machine's UDP sockets. 
# Bare minimum for all JSON packets is to have '_route' key/values.
# This is how the Hastur router will know where to route the message.
#
module Hastur
  module API
    extend self

    SECS_2100       = 4102444800
    MILLI_SECS_2100 = 4102444800000
    MICRO_SECS_2100 = 4102444800000000
    NANO_SECS_2100  = 4102444800000000000

    SECS_1971       = 31536000
    MILLI_SECS_1971 = 31536000000
    MICRO_SECS_1971 = 31536000000000
    NANO_SECS_1971  = 31536000000000000

    private

    #
    # Starts a background thread that will execute blocks of code every so often.
    #
    def start_client_thread
      @intervals = [:five_secs, :minute, :hour, :day]
      @interval_values = [5, 60, 60*60, 60*60*2 ]
      @last_time ||= Hash.new
      @scheduled_blocks ||= Hash.new
      # initialize all of the scheduling hashes
      @intervals.each do |interval|
        @last_time[interval] = Time.at(0)
        @scheduled_blocks[interval] = []
      end

      # add a heartbeat background job
      every :minute do
        heartbeat(nil, {:app => :client_heartbeat})
      end
      
      # define a thread that will schedule and execute all of the background jobs.
      # it is not very accurate on the scheduling, but should not be a problem
      @bg_thread = Thread.new do
        begin
          loop do
            idx = 0
            # for each of the interval buckets
            @intervals.each do |interval|
              curr_time = Time.now
              # execute the scheduled items if time is up
              if curr_time - @last_time[ interval ] >= @interval_values[idx]
                @last_time[ interval ] = curr_time
                @scheduled_blocks[ interval ].each { |b| b.call }
              end
              idx += 1
            end

            sleep 1       # rest
          end
        rescue Exception => e
          STDERR.puts e.inspect
        end
      end
    end

    protected

    #
    # Best effort to make all timestamps 64 bit numbers that represent the total number of
    # microseconds since Jan 1, 1970 at midnight UTC.
    #
    def normalize_timestamp(timestamp)
      timestamp = Time.now if timestamp.nil?

      case timestamp
      when Time
        (timestamp.to_f*1000000).to_i
      when SECS_1971..SECS_2100
        timestamp * 1000000
      when MILLI_SECS_1971..MILLI_SECS_2100
        timestamp * 1000
      when MICRO_SECS_1971..MICRO_SECS_2100
        timestamp
      when NANO_SECS_1971..NANO_SECS_2100
        timestamp / 1000
      else
        raise "Unable to validate timestamp: #{timestamp}"
      end
    end

    #
    # Returns the default labels for any UDP message that ships.
    #
    def default_labels
      @pid ||= Process.pid
      thread = Thread.current
      unless thread[:tid]
        thread[:tid] = thread_id(thread)
      end

      {
        :pid => @pid,
        :tid => thread[:tid],
        :app => app_name,
      }
    end

    #
    # This is a convenience function because the Ruby
    # thread API has no accessor for the thread ID,
    # but includes it in "to_s" (buh?)
    #
    def thread_id(thread)
      return 0 if thread == Thread.main

      str = thread.to_s

      match = nil
      match  = str.match /(0x\d+)/
      return nil unless match
      match[1].to_i
    end

    def app_name
      return @app_name if @app_name

      eco = Ecology rescue nil
      return @app_name = Ecology.application if eco

      @app_name = $0
    end

    def test_mode
      @test_mode || false
    end

    #
    # Get the UDP port.  Defaults to 8125.
    #
    def udp_port
      @udp_port || 8125
    end

    #
    # Sends a message unmolested to the HASTUR_UDP_PORT on 127.0.0.1
    #
    def send_to_udp(m)
      if @__test_mode__
        @__test_msgs__ ||= []
        @__test_msgs__ << m
      else
        u = ::UDPSocket.new
        u.send MultiJson.encode(m), 0, "127.0.0.1", udp_port
      end
    end

    public

    #
    # Returns a list of messages that were queued up when in test mode.
    #
    def __test_msgs__
      @__test_msgs__ ||= []
    end

    #
    # Clears the list of buffered messages.
    #
    def __clear_msgs__
      @__test_msgs__.clear
    end

    #
    # Switches the behavior of how messages gets handled. If test_mode is on, then 
    # all messages are buffered in memory instead of getting shipped through UDP.
    # Only use this method for testing purposes.
    #
    def __test_mode__=(test_mode)
      @__test_mode__ = test_mode
    end

    def app_name=(new_name)
      @app_name = new_name
    end

    #
    # Set the UDP port.  Defaults to 8125
    #
    def udp_port=(new_port)
      @udp_port = new_port
    end

    #
    # Sends a 'mark' stat to Hastur client daemon.
    #
    def mark(name, timestamp=Time.now, labels = {})
      send_to_udp :_route    => :stat,
                  :type      => :mark,
                  :name      => name,
                  :timestamp => normalize_timestamp(timestamp),
                  :labels    => default_labels.merge(labels)
    end

    #
    # Sends a 'counter' stat to Hastur client daemon.
    #
    def counter(name, increment = 1, timestamp=Time.now, labels = {})
      send_to_udp :_route    => :stat,
                  :type      => :counter,
                  :name      => name,
                  :timestamp => normalize_timestamp(timestamp),
                  :increment => increment,
                  :labels    => default_labels.merge(labels)
    end

    #
    # Sends a 'gauge' stat to Hastur client daemon.
    #
    def gauge(name, value, timestamp=Time.now, labels = {})
      send_to_udp :_route    => :stat,
                  :type      => :gauge,
                  :name      => name,
                  :timestamp => normalize_timestamp(timestamp),
                  :value     => value,
                  :labels    => default_labels.merge(labels)
    end

    #
    # Constructs and sends a notify UDP packet
    #
    def notification(message, labels = {})
      send_to_udp :_route  => :notification,
                  :message => message,
                  :labels  => default_labels.merge(labels)
    end

    #
    # Constructs and sends a register_plugin UDP packet
    #
    def register_plugin(plugin_path, plugin_args, plugin_name, interval, labels = {})
      send_to_udp :_route      => :register_plugin,
                  :plugin_path => plugin_path,
                  :plugin_args => plugin_args,
                  :interval    => interval,
                  :plugin      => plugin_name,
                  :labels      => default_labels.merge(labels)
    end

    #
    # Constructs and sends a register_service UDP packet
    #
    def register_service(labels = {})
      send_to_udp :_route => :register_service,
                  :labels => default_labels.merge(labels)
    end

    #
    # Constructs and sends heartbeat UDP packets.
    #
    def heartbeat(timestamp = Time.now, labels = {})
      send_to_udp :_route    => :heartbeat_client,
                  :timestamp => normalize_timestamp(timestamp),
                  :labels    => default_labels.merge(labels)
    end

    #
    # Runs a block of code every so often, which is defined by interval. 
    # Use this method to report statistics at a fixed time interval.
    #
    def every( interval, &block )
      raise "Interval must be one of these: #{@intervals}, you gave #{interval.inspect}" unless @intervals.include?(interval)
      @mutex ||= Mutex.new
      @mutex.synchronize { @scheduled_blocks[interval] << block }
    end

    # Automatically start the background thread for the client.
    start_client_thread
  end
end
