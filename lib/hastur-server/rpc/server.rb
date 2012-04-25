require 'ffi-rzmq'
require 'termite'

require "hastur-server/message"
require "hastur-server/util"

module Hastur
  module RPC
    class Server
      attr_reader :socket, :uri
      attr_accessor :running

      #
      # Create a new simple RPC server.
      # @param [String] URI of the RPC server
      # @param [ZMQ::Context] optional zeromq context object
      #
      def initialize(uri, ctx=nil)
        @uri = uri
        @handlers = {}
        @running = true

        @logger = Termite::Logger.new

        @ctx = ctx || ZMQ::Context.new
        @poller = ZMQ::Poller.new

        @socket = @ctx.socket ZMQ::REP
        @socket.setsockopt ZMQ::LINGER, -1
        @socket.setsockopt ZMQ::HWM, 2
      end

      #
      # Register a block to handle a JSON-RPC method
      # @param [String,Symbol] JSON-RPC method to handle
      # @yield [Hash] block will be called for ever message with the provided method and
      #               handed the complete deserialized JSON
      #
      def add_handler(method, &block)
        @handlers[method.to_s] = block
      end

      #
      # poll the zmq sockets, either timing out or handling any available messages
      # @param [Float] poll timeout
      #
      def poll_zmq zmq_poll_timeout
        rc = @poller.poll zmq_poll_timeout
        return unless ZMQ::Util.resultcode_ok?(rc)

        @poller.readables.each do |socket|
          rc = socket.recv_strings messages=[]

          unless ZMQ::Util.resultcode_ok?(rc)
            @logger.error "recv error: #{ZMQ::Util.error_string}"
            return
          end

          begin
            data = MultiJson.load(messages[-1], :symbolize_keys => true)
            raise "Received JSON but it does not look like JSON-RPC!" unless data[:method]
          rescue Exception => e
            @logger.error "invalid message: #{messages.inspect}\n#{e}"
            return -1
          end

          begin
            result = @handlers[data[:method].to_s].call data
            respond(result)
          rescue Exception => e
            @logger.error "handler block [#{data[:method]}] failed: #{e}"
            respond(result, e)
          end
        end
      end

      #
      # run in a loop while .running == true
      # @param [Float] poll timeout
      #
      def run(zmq_poll_timeout=1)
        rc = @socket.bind @uri
        raise "socket bind error: #{ZMQ::Util.error_string}" unless ZMQ::Util.resultcode_ok?(rc)
        rc = @poller.register_readable @socket
        raise "socket poller error: #{ZMQ::Util.error_string}" unless ZMQ::Util.resultcode_ok?(rc)

        @running = true
        while @running == true
          poll_zmq zmq_poll_timeout
        end
      end

      private

      #
      # format & send a JSON response
      # @param [Hash] response data (to put inside the JSON-RPC message)
      # @return [String] json!
      #
      def respond(data, error=nil)
        resdata = { :result => data, :error => error }
        json = MultiJson.dump resdata
        rc = socket.send_strings [json]
        raise "socket send error: #{ZMQ::Util.error_string}" unless ZMQ::Util.resultcode_ok?(rc)
        json
      end
    end
  end
end
