#
# Singleton queue that will manage the states of this client's notifications.
#

require "singleton"
require "#{File.dirname(__FILE__)}/hastur_logger"

class HasturNotificationQueue
  include Singleton

  attr_accessor :notifications, :resend_thread

  # TODO(viet): dynamically get this parameter too
  NOTIFICATION_QUEUE_TIMEOUT=5

  def initialize
    @notifications = Hash.new
  end

  def run
    HasturLogger.instance.log("Starting notification queue.")
    start_resend_thread
  end

  #
  # Remove a notification object from the Queue.
  #
  def remove(notification_id)
    notification = @notifications.delete(notification_id)
    if notification.nil?
      HasturLogger.instance.log("Successfully removed #{notification_id} from the notification queue.")
    else
      HasturLogger.instance.log("Notification #{notification_id} does not exist on this client's notification queue.")
    end
  end

  #
  # Adds a notification object to the Queue. 
  #
  def add(notification)
    @notifications[notification.id] = notification
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
          HasturLogger.instance.error("A problem occurred when trying to resend notifications.")
        end
      end
    end
  end

  #
  # Resends all notifications that are in the queue. This means that there are notifications
  # that have not yet been ack'd. 
  #
  def resend
    @notifications.each do |id, notification|
      HasturMessenger.instance.send( notification.to_json )
    end
    HasturLogger.instance.log("Re-sending #{@notifications.size} notifications") unless @notifications.empty?
  end

end
