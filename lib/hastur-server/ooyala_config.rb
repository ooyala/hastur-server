require 'resolv'
require 'httparty'

module Hastur
  module OoyalaConfig
    # we run our routers on tcp://0.0.0.0:8126
    ROUTER_PORT = 8126

    # if all else fails, use this list
    DEFAULT_ROUTERS = %w[
      hastur-core1.us-east-1.ooyala.com
      hastur-core2.us-east-1.ooyala.com
      hastur-core3.us-east-1.ooyala.com
    ].freeze

    #
    # Get a list of routers for sending metrics to.
    #
    # @return [Array<String>] list of ZeroMQ URIs
    #
    def get_routers
      # check for ec2 / find what region it's in
      rr_name = begin
        req = HTTParty.get 'http://169.254.169.254/latest/meta-data/placement/availability-zone', :timeout => 5
        if req.code == 200 and /\A(?<region>\w+-\w+-\d+)[a-z]+\Z/ =~ req.body
          "hastur-core.#{region}.ooyala.com"
        end
      rescue
        "hastur-core.us-east-1.ooyala.com"
      end

      # look up the round-robin record to get all the router addresses
      routers = begin
        names = Resolv.getaddresses(rr_name)
        names.any? ? names : DEFAULT_ROUTERS
      rescue
        DEFAULT_ROUTERS
      end

      routers.map { |router| "tcp://#{router}:#{ROUTER_PORT}" }
    end
  end
end
