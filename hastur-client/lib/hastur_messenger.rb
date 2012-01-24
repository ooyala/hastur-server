#
# A class for all of the hastur-client to talk to the message bus
#

require "ffi-rzmq"
require_relative "client_config"

class HasturMessenger

  class << self
    # TODO(viet): figure out how to dynamically retrieve this from puppet or whatever deploys this client agent
    LINK="tcp://127.0.0.1:#{HasturClientConfig::HASTUR_CLIENT_ZMQ_PORT}"

    attr_accessor :socket, :uuid, :context

    def set_uuid(uuid)
      HasturMessenger.uuid = uuid
    end

    def send(topic, msg)
      if HasturMessenger.socket.nil?
        HasturMessenger.context = ZMQ::Context.new if HasturMessenger.context.nil?
        HasturMessenger.socket = HasturMessenger.context.socket(ZMQ::DEALER)
        HasturMessenger.socket.setsockopt(ZMQ::IDENTITY, @uuid)
        HasturMessenger.socket.connect( LINK )
      end
      # only for debugging purposes
      STDOUT.puts "client => Pretending to send => [#{topic}] #{msg}"
      payload_msg = ZMQ::Message.new(msg)
      topic_msg = ZMQ::Message.new(topic)
      HasturMessenger.socket.send(topic_msg, ZMQ::SNDMORE)
      HasturMessenger.socket.send(payload_msg)
    end
  end
end 
