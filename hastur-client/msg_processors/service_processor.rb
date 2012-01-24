#
# The HasturServiceProcessor will register a service with Hastur.
#

require_relative "../lib/client_config"
require_relative "message_processor"

class HasturServiceProcessor < HasturMessageProcessor
  
  REGISTER_SERVICE="register_service"

  def initialize
    super( REGISTER_SERVICE )
  end

  #
  # Checks if the message is a REGISTER_SERVICE type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur(HasturClientConfig::REGISTER_ROUTE, msg.to_json)
      return true
    end
    return false
  end
end
