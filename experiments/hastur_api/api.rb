require "multi_json"

#
# Hastur API gem that allows services/apps to easily publish
# correct Hastur-commands to their local machine's UDP sockets. 
#

# TODO: Potentially figure out how to get ecology stuff.
# TODO: Figure out how to get the service/app name on the fly (ecology?)
# TODO: Figure out the proper JSON format for all UDP messages
module Hastur
  module API
    extend self

    HASTUR_UDP_PORT=8125

    #
    # Constructs and sends a stat UDP packet
    #
    def stat(type, name, stat, unit, tags)
      m = {
            :type => type,
            :name => name,
            :stat => stat,
            :unit => unit,
            :tags => tags
          }.to_json
      
      send_to_udp(m)
    end

    #
    # Constructs and sends a notify UDP packet
    #
    def notify(message)
      m = {
            :message => message
          }

      send_to_udp(message)
    end
 
    #
    # Constructs and sends a heartbeat UDP packet
    #
    def heartbeat(name, interval)
      m = {
            :name => name,
            :interval => interval
          }
      send_to_udp(m)
    end
   
    #
    # Constructs and sends a register_service UDP packet
    #
    def register_service
      m = {
            :app => # TODO: get the app name somewhere (ecology?)
          }

      send_to_udp(message)
    end

    #
    # Sends a message unmolested to the HASTUR_UDP_PORT on 127.0.0.1
    #
    def send_to_udp(m)
      u = UDPSocket.new
      u.send m, 0, "127.0.0.1", HASTUR_UDP_PORT
    end

  end
end
