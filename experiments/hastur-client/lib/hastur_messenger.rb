#
# A class for all of the hastur-client to talk to the message bus
#

require "ffi-rzmq"
require_relative "client_config"

class HasturMessenger
  class << self
    attr_accessor :socket, :uuid, :context

    # ZMQ::Context should only be done once per process.
    # Sockets only need to be created once per URI for the lifetime of the process.
    def initialize
      if @context.nil?
        @context = ZMQ::Context.new
      end

      @socket = @context.socket(ZMQ::DEALER)
      @socket.setsockopt(ZMQ::IDENTITY, @uuid)

      # eventually, this will move to using a list queried from a 2/3 node cluster of
      # naming devices that simply returns a list of routers
      # for now, this only ever needs to be done once per process
      HasturClientConfig::HASTUR_CLIENT_ZMQ_ROUTERS.each do |router_uri|
        @socket.connect router_uri
      end
    end

    def set_uuid(uuid)
      @uuid = uuid

      unless @socket.nil?
        @socket.setsockopt(ZMQ::IDENTITY, @uuid)
      end
    end

    def send(topic, msg)
      self.initialize if @context.nil? or @socket.nil?

      payload_msg = ZMQ::Message.new(msg)
      topic_msg = ZMQ::Message.new("v1\n#{topic}\nack:none")
      HasturMessenger.socket.send(topic_msg, ZMQ::SNDMORE)
      HasturMessenger.socket.send(payload_msg)
      puts "[#{topic}] #{msg}"
    end
  end
end 
