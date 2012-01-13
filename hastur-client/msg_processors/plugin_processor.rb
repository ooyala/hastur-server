#
# The HasturPluginProcessor will register a service with Hastur.
#

require "#{File.dirname(__FILE__)}/message_processor"

class HasturPluginProcessor < HasturMessageProcessor
  
  REGISTER_PLUGIN="register_plugin"
  REGISTRATION_TOPIC="/topic/hastur/register"

  def initialize
    super( REGISTER_PLUGIN )
  end

  #
  # Checks if the message is a REGISTER_PLUGIN type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur(REGISTRATION_TOPIC, msg)
      return true
    end
    return false
  end
end
