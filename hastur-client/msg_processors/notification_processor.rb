#
# The HasturNotificationProcessor will send a notification/alert to Hastur.
#

require "#{File.dirname(__FILE__)}/message_processor"
require "#{File.dirname(__FILE__)}/../models/hastur_notification"

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
      # queue notification in case something happens
      name = msg['params']['name']
      subsystem = msg['params']['name']
      uuid = msg['params']['uuid']
      notification = Hastur::Notification.new(name, subsystem, uuid)
      HasturNotificationQueue.instance.add( notification )
      # tell Hastur about this horrible incident
      flush_to_hastur( notification.to_json )
      return true
    end
    return false
  end
end
