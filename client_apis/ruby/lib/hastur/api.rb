require "multi_json"
require "socket"

#
# Hastur API gem that allows services/apps to easily publish
# correct Hastur-commands to their local machine's UDP sockets. 
# Bare minimum for all JSON packets is to have '_route' key/values.
# This is how the Hastur router will know where to route the message.
#

# TODO: Potentially figure out how to get ecology stuff.
# TODO: Figure out how to get the service/app name on the fly (ecology?)
# TODO: Figure out the proper JSON format for all UDP messages
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

    #
    # Allow the UDP port to be configurable. Defaults to 8125.
    #
    def udp_port
      @udp_port || 8125
    end

    #
    # Best effort to make all timestamps 64 bit numbers that represent the total number of
    # microseconds since the beginning of 1971.
    #
    def normalize_timestamp(timestamp)
      return timestamp.to_f*1000000 if timestamp.kind_of?(Hash)
      return timestamp * 1000000    if timestamp.between?(SECS_1971, SECS_2100)
      return timestamp * 1000       if timestamp.between?(MILLI_SECS_1971, MILLI_SECS_2100)
      return timestamp              if timestamp.between?(MICRO_SECS_1971, MICRO_SECS_2100)
      return timestamp / 1000       if timestamp.between?(NANO_SECS_1971, NANO_SECS_2100)
      # if the program made it here, raise an error. Do not know what to do with this timestamp.
      raise "Unable to validate timestamp: #{timestamp}"
    end

    #
    # Sends a 'mark' stat to Hastur client daemon.
    #
    def mark(name, timestamp=normalize_timestamp(Time.now), labels = {})
      m = {
            :_route    => "stat",
            :type      => "mark",
            :name      => name,
            :timestamp => timestamp,
            :labels    => labels
          }
      send_to_udp(m)
    end

    #
    # Sends a 'counter' stat to Hastur client daemon.
    #
    def counter(name, increment, timestamp=normalize_timestamp(Time.now), labels = {})
      normalize_timestamp(timestamp)
      m = {
            :_route    => "stat",
            :type      => "counter",
            :name      => name,
            :timestamp => timestamp,
            :increment => increment,
            :labels    => labels
          }
      send_to_udp(m)
    end

    #
    # Sends a 'gauge' stat to Hastur client daemon.
    #
    def gauge(name, value, timestamp=normalize_timestamp(Time.now), labels = {})
      normalize_timestamp(timestamp)
       m = {
            :_route    => "stat",
            :type      => "gauge",
            :name      => name,
            :timestamp => timestamp,
            :value     => value,
            :labels    => labels
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a notify UDP packet
    #
    def notification(message)
      m = {
            :_route  => "notification",
            :message => message
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a register_plugin UDP packet
    #
    def register_plugin(plugin_path, plugin_args, plugin_name, interval)
      m = {
            :_route      => "register_plugin",
            :plugin_path => plugin_path,
            :plugin_args => plugin_args,
            :interval    => interval,
            :plugin      => plugin_name
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a register_service UDP packet
    #
    def register_service(app)
      m = {
            :_route => "register_service",
            :app    => app # TODO: get the app name somewhere (ecology?)
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends heartbeat UDP packets. Interval is given in seconds.
    #
    def heartbeat(app, interval)
      if @heartbeat_thread.nil?
        @heartbeat_thread = Thread.new do
          m = {
                :_route   => "heartbeat",
                :app      => app, # TODO: get the app name somewhere (ecology?)
                :interval => interval
              }
          loop do
            send_to_udp(m)
            sleep interval
          end
        end
      end
    end

    #
    # Sends a message unmolested to the HASTUR_UDP_PORT on 127.0.0.1 #
    #
    def send_to_udp(m)
      u = ::UDPSocket.new
      u.send MultiJson.encode(m), 0, "127.0.0.1", udp_port
    end

  end
end
