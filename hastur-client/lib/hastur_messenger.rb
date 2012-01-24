#
# A singleton class for all of the hastur-client to talk to the message bus
#

require "singleton"
require "ffi-rzmq"

class HasturMessenger

  class << self
    # TODO(viet): figure out how to dynamically retrieve this from puppet or whatever deploys this client agent
    LINK="tcp://127.0.0.1:8000"

    attr_accessor :socket, :uuid, :context

    def set_uuid(uuid)
      HasturMessenger.uuid = uuid
    end

    def send(msg)
      if HasturMessenger.socket.nil?
        HasturMessenger.context = ZMQ::Context.new if HasturMessenger.context.nil?
        HasturMessenger.socket = HasturMessenger.context.socket(ZMQ::DEALER)
        HasturMessenger.socket.setsockopt(ZMQ::IDENTITY, @uuid)
        HasturMessenger.socket.connect( LINK )
      end
      # only for debugging purposes
      STDOUT.puts "client => Pretending to send => #{msg}"
      zmq_msg = ZMQ::Message.new(msg)
      HasturMessenger.socket.send(zmq_msg)
    end
  end
end 
