require "multi_json"
require "socket"

#
# Hastur API gem that allows services/apps to easily publish
# correct Hastur-commands to their local machine's UDP sockets. 
# Bare minimum for all JSON packets is to have 'method' key/values.
#

# TODO: Potentially figure out how to get ecology stuff.
# TODO: Figure out how to get the service/app name on the fly (ecology?)
# TODO: Figure out the proper JSON format for all UDP messages
module Hastur
  module API
    extend self

    #
    # Allow the UDP port to be configurable. Defaults to 8125.
    #
    def udp_port
      @udp_port || 8125
    end

    #
    # Constructs and sends a stat UDP packet
    #
    def stat(name, stat, unit, tags)
      m = {
            :method => "stat",
            :name   => name,
            :stat   => stat,
            :unit   => unit,
            :tags   => tags
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a notify UDP packet
    #
    def notification(message)
      m = {
            :method  => "notification",
            :message => message
          }
      send_to_udp(m)
    end
 
    #
    # Constructs and sends a heartbeat UDP packet
    #
    def heartbeat_service(name, interval)
      m = {
            :method   => "heartbeat_service",
            :name     => name,
            :interval => interval
          }
      send_to_udp(m)
    end

    #
    # Constructs and sends a register_plugin UDP packet
    #
    def register_plugin(plugin_path, plugin_args, plugin_name, interval)
      m = {
            :method      => "register_plugin",
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
            :method => "register_service",
            :app    => app # TODO: get the app name somewhere (ecology?)
          }
      send_to_udp(m)
    end

    #
    # Sends a message unmolested to the HASTUR_UDP_PORT on 127.0.0.1 #
    def send_to_udp(m)
      u = ::UDPSocket.new
      u.send MultiJson.encode(m), 0, "127.0.0.1", udp_port
    end

  end
end
