require 'ffi-rzmq'
require 'termite'
require "hastur-server/message"
require "hastur-server/util"

module Hastur
  module RPC
    class Client
      attr_reader :socket, :uri

      #
      # Create a new simple RPC client.
      # @param [String,Array<String>] URI(s) of the RPC server
      # @param [ZMQ::Context] optional zeromq context object
      #
      def initialize(uri, ctx=nil)
        @uri = uri.kind_of?(Array) ? uri : [uri]
        @logger = Termite::Logger.new

        @ctx = ctx || ZMQ::Context.new

        @socket = @ctx.socket ZMQ::REQ
        @socket.setsockopt ZMQ::LINGER, -1
        @socket.setsockopt ZMQ::HWM, 2

        @uri.each do |server|
          rc = @socket.connect server
          raise "socket connect error: #{ZMQ::Util.error_string}" unless ZMQ::Util.resultcode_ok?(rc)
        end
      end

      #
      # Call a remote method on a zeromq req/rep socket.
      # @param [String,Symbol] method to call on remote side
      # @param [Hash,Array] parameters for remote method
      #
      def request(method, params)
        req_data = { :method => method, :params => params }
        req_json = MultiJson.encode req_data
        rc = @socket.send_string req_json
        raise "socket send error: #{ZMQ::Util.error_string}" unless ZMQ::Util.resultcode_ok?(rc)
        rc = @socket.recv_string resp_json=""
        raise "socket recv error: #{ZMQ::Util.error_string}" unless ZMQ::Util.resultcode_ok?(rc)
        # should this handle messages with an error?
        response = MultiJson.decode resp_json, :symbolize_keys => true
        if response[:error]
          raise response[:error]
        else
          response[:result]
        end
      end
    end
  end
end
