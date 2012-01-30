#
# The HasturPluginProcessor will register a service with Hastur.
#

require_relative "message_processor"
require_relative "../lib/client_config"

class HasturPluginProcessor < HasturMessageProcessor
  
  REGISTER_PLUGIN="register_plugin"

  def initialize
    super( REGISTER_PLUGIN )
  end

  #
  # Checks if the message is a REGISTER_PLUGIN type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur(HasturClientConfig::REGISTER_ROUTE, msg.to_json)
      return true
    end
    return false
  end
end
