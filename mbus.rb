# This is a prototype for the message bus interface for the new Ooyala
# monitoring system being designed by the Tools and Automation Team.

# This interface is currently only asynchronous.  Want synchronous?
# Use "t = Thread.new { async_method { /* handler */ }; sleep 10000 }; t.join(SECONDS_TO_WAIT) || t.kill".
#

module MBus

  module Topic
    def send(topic)
    end

    # This subscribes asynchronously to only this
    # topic.  Without a block, subscribe for
    # general delivery.
    def receive_async(topic)
    end
  end

  module Queue
    def send(queue_name)
    end

    # This subscribes asynchronously to only this
    # queue.  Without a block, subscribe for
    # general delivery.
    def receive_async(queue_name)
    end
  end

  module Direct
    def send_uuid(uuid)
    end
  end

  # This subscribes to direct messages, and subscriptions for
  # general delivery.
  def subscribe_async
  end

end
