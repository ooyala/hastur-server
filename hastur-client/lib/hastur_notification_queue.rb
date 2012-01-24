#
# Singleton queue that will manage the states of this client's notifications.
#

require_relative "hastur_logger"

class HasturNotificationQueue

  class << self
    attr_accessor :notifications, :resend_thread

    # TODO(viet): dynamically get this parameter too
    NOTIFICATION_QUEUE_TIMEOUT=5

    HasturNotificationQueue.notifications = Hash.new

    def run
      HasturLogger.log("Starting notification queue.")
      start_resend_thread
    end

    def stop(clear_queue = false)
      HasturLogger.log("Stopping the notification queue.")
      HasturNotificationQueue.notifications.clear if clear_queue
      Thread.kill(@resend_thread)
      @resend_thread = nil
    end

    #
    # Remove a notification object from the Queue.
    #
    def remove(notification_id)
      notification = HasturNotificationQueue.notifications.delete(notification_id)
      if notification.nil?
        HasturLogger.log("Notification #{notification_id} does not exist on this client's notification queue.")
      else
        HasturLogger.log("Successfully removed #{notification_id} from the notification queue.")
      end
    end

    #
    # Adds a notification object to the Queue. 
    #
    def add(notification)
      HasturNotificationQueue.notifications[notification.id] = notification
    end

    #
    # Starts a thread that will periodically clean up the notification queue by resending them. 
    # When acks are received then they should be removed from the queue.
    #
    def start_resend_thread
      if @resend_thread.nil?
        @resend_thread = Thread.start do
          begin
            loop do
              sleep NOTIFICATION_QUEUE_TIMEOUT
              resend()
            end
          rescue Exception => e
            HasturLogger.error("A problem occurred when trying to resend notifications.")
          end
        end
      end
    end

    #
    # Resends all notifications that are in the queue. This means that there are notifications
    # that have not yet been ack'd. 
    #
    def resend
      HasturNotificationQueue.notifications.each do |id, notification|
        HasturMessenger.send("notification", notification.to_json)
      end
      HasturLogger.log("Re-sending #{HasturNotificationQueue.notifications.size} notifications") unless HasturNotificationQueue.notifications.empty?
    end
  end
end
