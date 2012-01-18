#
# The HasturNotificationProcessor will send a notification/alert to Hastur.
#

require "#{File.dirname(__FILE__)}/message_processor"

class HasturNotificationProcessor < HasturMessageProcessor
  
  NOTIFICATION="notification"

  def initialize
    super( NOTIFICATION )
  end

  #
  # Checks if the message is a NOTIFICATION type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur(msg.to_json)
      return true
    end
    return false
  end
end
