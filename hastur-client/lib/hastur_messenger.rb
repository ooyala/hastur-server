#
# A singleton class for all of the hastur-client to talk to the message bus
#

require "singleton"
require "#{File.dirname(__FILE__)}/../../hastur-mq/lib/hastur-mq"

class HasturMessenger
  include Singleton

  # TODO(viet): figure out how to dynamically retrieve this from puppet or whatever deploys this client agent
  LINK="tcp://127.0.0.1:8000"

  # TODO(viet): Include the MQ stuff in here
  attr_accessor :socket, :uuid

  def set_uuid(uuid)
    @uuid = uuid
  end

  def send(msg)
    if @socket.nil? 
      @socket = HasturMq::Dealer.new(LINK, @uuid)
    end
    # TODO(viet): implement this once the MQ wrapper is available
    STDOUT.puts "client => Pretending to send => #{msg}"
    @socket.send(msg)
  end
end 
