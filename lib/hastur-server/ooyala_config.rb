require 'socket'
require 'resolv'
require 'httparty'

# This module is used by the Bluepill scripts to set up the list of routers
# per-region, specifically for Ooyala.  Clearly this doesn't belong in the
# released GitHub code -- it's not a security risk, but it *is* useless to
# other folks.  When/if we integrate Hastur properly into our Chef
# infrastructure this code will go away.

module Hastur
  module OoyalaConfig
    extend self

    # we run our routers on tcp://0.0.0.0:8126
    ROUTER_PORT = 8126

    ROUTERS_BY_REGION = {
      "us-east-1" => [ "bridge1.us-east-1.ooyala.com", "bridge2.us-east-1.ooyala.com" ],
      "eu-west-1" => [ "bridge1.eu-west-1.ooyala.com", "bridge2.eu-west-1.ooyala.com" ],
      "us-west-2" => [ "bridge1.us-west-2.ooyala.com", "bridge2.us-west-2.ooyala.com" ],
      "syd1" => [ "bridge1.syd1", "bridge2.syd1" ],
      "sv2" => [ "hastur-core.sv2" ]
    }

    #
    # Get a list of routers for sending metrics to.
    #
    # @return [Array<String>] list of ZeroMQ URIs
    #
    def get_routers
      hostname = Socket.gethostname

      if hostname =~ /\.(?:sv2|mtv)\Z/
        region = "sv2"
      elsif `which my-region` != ""
        region = `my-region`.chomp rescue nil
      elsif File.exist?("/bin/myregion")
        region = `/bin/my-region`.chomp rescue nil
      else
        region = "us-east-1"  # Fallback
      end

      # look up the round-robin record to get all the router addresses
      routers = (ROUTERS_BY_REGION[region] || []).flat_map { |name| Resolv.getaddresses(name) }
      routers = ROUTERS_BY_REGION["us-east-1"] if routers.empty?  # Fallback

      routers.map { |router| "tcp://#{router}:#{ROUTER_PORT}" }
    end
  end
end
