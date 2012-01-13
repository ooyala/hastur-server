#
# The HasturServiceProcessor will register a service with Hastur.
#

require "#{File.dirname(__FILE__)}/message_processor"

class HasturServiceProcessor < HasturMessageProcessor
  
  REGISTER_SERVICE="register_service"
  REGISTRATION_TOPIC="/topic/hastur/register"

  def initialize
    super( REGISTER_SERVICE )
  end

  #
  # Checks if the message is a REGISTER_SERVICE type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur(REGISTRATION_TOPIC, msg)
      return true
    end
    return false
  end
end
