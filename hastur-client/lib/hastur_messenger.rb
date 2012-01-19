#
# A singleton class for all of the hastur-client to talk to the message bus
#

require "singleton"
require "ffi-rzmq"

class HasturMessenger
  include Singleton

  # TODO(viet): figure out how to dynamically retrieve this from puppet or whatever deploys this client agent
  LINK="tcp://127.0.0.1:8000"

  attr_accessor :socket, :uuid, :context

  def set_uuid(uuid)
    @uuid = uuid
  end

  def send(msg)
    if @socket.nil?
      @context = ZMQ::Context.new if @context.nil?
      @socket = context.socket(ZMQ::DEALER)
      @socket.setsockopt(ZMQ::IDENTITY, @uuid)
      @socket.connect( LINK )
    end
    STDOUT.puts "client => Pretending to send => #{msg}"
    zmq_msg = ZMQ::Message.new(msg)
    @socket.send(zmq_msg)
  end
end 
