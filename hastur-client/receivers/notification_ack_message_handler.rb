#
# Handles any incoming messages with 'method' set to notification_ack. This handler
# will process the message by removing the notification from the client's queue.
#

require "rubygems"
require "json"
require_relative "../lib/hastur_logger"
require_relative "../lib/hastur_messenger"
require_relative "../lib/hastur_notification_queue"

class NotificationAckMessageHandler
  def self.handle(message)
    begin
      msg = JSON.parse(message)
      HasturLogger.log("Attempting to remove notification #{msg['id']} from queue.")
      HasturNotificationQueue.remove(msg['id'])
    rescue Exception => e
      HasturLogger.error("Unable to process message #{message}.\n#{e.message}\n#{e.backtrace}")
    end
  end
end
