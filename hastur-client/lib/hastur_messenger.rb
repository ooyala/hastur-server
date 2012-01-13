#
# A singleton class for all of the hastur-client to talk to the message bus
#

require "singleton"

class HasturMessenger
  include Singleton

  # TODO(viet): Include the MQ stuff in here

  def send(topic_name, msg)
    # TODO(viet): implement this once the STOMP wrapper is available
    STDOUT.puts "Pretending to send on topic #{topic_name} => #{msg}"
  end
end 
