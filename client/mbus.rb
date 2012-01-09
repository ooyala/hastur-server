# This is a prototype for the message bus interface for the new Ooyala
# monitoring system being designed by the Tools and Automation Team.

# This interface is currently only asynchronous.  Want synchronous?
# Use "t = Thread.new { async_method { /* handler */ }; sleep 10000 }; t.join(SECONDS_TO_WAIT) || t.kill".
#

require "onstomp"
require "multi_json"

module MBus
  def connect(*args)
    @stomp_client = OnStomp.connect(*args)
  end

  module Topic
    def send(topic, json_hash)
      @stomp_client.send(topic, MultiJson.encode(json_hash))
    end

    # This subscribes asynchronously to only this
    # topic.  Without a block, subscribe for
    # general delivery.
    def receive_async(topic, &block)
      if block_given?
        @stomp_client.subscribe(topic, :ack => :none, &block)
      else
        raise "Need block for now!"
      end
    end
  end

  module Queue
    def send(queue_name, json_hash)
      @stomp_client.send(queue_name, MultiJson.encode(json_hash))
    end

    # This subscribes asynchronously to only this
    # queue.  Without a block, subscribe for
    # general delivery.
    def receive_async(queue_name)
      if block_given?
        @stomp_client.subscribe(topic, :ack => :client, &block)
      else
        raise "Need block for now!"
      end
    end
  end

  module Direct
    def send_uuid(uuid)
      raise "No direct messages implemented yet!"
    end
  end

  # This subscribes to direct messages, and subscriptions for
  # general delivery.
  def subscribe_async
    raise "General delivery not implemented yet!"
  end

end
