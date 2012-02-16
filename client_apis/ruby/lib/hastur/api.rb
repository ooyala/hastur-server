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

    protected

    #
    # Best effort to make all timestamps 64 bit numbers that represent the total number of
    # microseconds since the beginning of 1971.
    #
    def normalize_timestamp(timestamp)
      timestamp = Time.now if timestamp.nil?

      return timestamp.to_f*1000000 if timestamp.kind_of?(Time)
      return timestamp * 1000000    if timestamp.between?(SECS_1971, SECS_2100)
      return timestamp * 1000       if timestamp.between?(MILLI_SECS_1971, MILLI_SECS_2100)
      return timestamp              if timestamp.between?(MICRO_SECS_1971, MICRO_SECS_2100)
      return timestamp / 1000       if timestamp.between?(NANO_SECS_1971, NANO_SECS_2100)
      # if the program made it here, raise an error. Do not know what to do with this timestamp.
      raise "Unable to validate timestamp: #{timestamp}"
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

    # This is a convenience function because the Ruby
    # thread API has no accessor for the thread ID,
    # but includes it in "to_s" (buh?)
    def thread_id(thread)
      return "main" if thread == Thread.main

      str = thread.to_s

      match = nil
      match  = str.match /(0x\d+)/
      return nil unless match
      match[1]
    end

    public

    def app_name=(new_name)
      @app_name = new_name
    end

    #
    # Set the UDP port.  Defaults to 8125
    #
    def udp_port=(new_port)
      @udp_port = new_port
    end

    protected

    def app_name
      return @app_name if @app_name

      eco = Ecology rescue nil
      return @app_name = Ecology.application if eco

      @app_name = $0
    end

    #
    # Get the UDP port.  Defaults to 8125.
    #
    def udp_port
      @udp_port || 8125
    end

    #
    # Sends a message unmolested to the HASTUR_UDP_PORT on 127.0.0.1 #
    #
    def send_to_udp(m)
      u = ::UDPSocket.new
      u.send MultiJson.encode(m), 0, "127.0.0.1", udp_port
    end

    public

    #
    # Sends a 'mark' stat to Hastur client daemon.
    #
    def mark(name, timestamp=Time.now, labels = {})
      m = {
            :_route    => "stat",
            :type      => "mark",
            :name      => name,
            :timestamp => normalize_timestamp(timestamp),
            :labels    => default_labels.merge(labels)
          }
      send_to_udp(m)
    end

    #
    # Sends a 'counter' stat to Hastur client daemon.
    #
    def counter(name, increment = 1, timestamp=Time.now, labels = {})
      m = {
            :_route    => "stat",
            :type      => "counter",
            :name      => name,
            :timestamp => normalize_timestamp(timestamp),
            :increment => increment,
            :labels    => default_labels.merge(labels),
          }
      send_to_udp(m)
    end

    #
    # Sends a 'gauge' stat to Hastur client daemon.
    #
    def gauge(name, value, timestamp=Time.now, labels = {})
      m = {
            :_route    => "stat",
            :type      => "gauge",
            :name      => name,
            :timestamp => normalize_timestamp(timestamp),
            :value     => value,
            :labels    => default_labels.merge(labels),
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a notify UDP packet
    #
    def notification(message, labels = {})
      m = {
            :_route  => "notification",
            :message => message,
            :labels  => default_labels.merge(labels)
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a register_plugin UDP packet
    #
    def register_plugin(plugin_path, plugin_args, plugin_name, interval, labels = {})
      m = {
            :_route      => "register_plugin",
            :plugin_path => plugin_path,
            :plugin_args => plugin_args,
            :interval    => interval,
            :plugin      => plugin_name,
            :labels      => default_labels.merge(labels),
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a register_service UDP packet
    #
    def register_service(labels = {})
      m = {
            :_route => "register_service",
            :labels => default_labels.merge(labels),
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends heartbeat UDP packets. Interval is given in seconds.
    #
    def heartbeat(name = "app_heartbeat", timestamp = Time.now, labels = {})
      send_to_udp({
        :_route    => "heartbeat",
        :timestamp => normalize_timestamp(timestamp),
        :labels    => default_labels.merge(labels),
      })
    end

  end
end
