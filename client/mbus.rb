# This is a prototype for the message bus interface for the new Ooyala
# monitoring system being designed by the Tools and Automation Team.

# This interface is currently only asynchronous.  Want synchronous?
# Use "t = Thread.new { async_method { /* handler */ }; sleep 10000 }; t.join(SECONDS_TO_WAIT) || t.kill".
#

require "onstomp"
require "multi_json"

module MBus
  def connect
    @hastur_settings = MultiJson.decode(File.read("/etc/hastur.json"))
    @domain = @hastur_settings["domain"] || "us-west-2.ooyala.com"

    @hostname = @hastur_settings["hostname"] || `hostname`
    @protocol = @hastur_settings["protocol"] || ""

    # TODO(noah): Set UUID

    clients = (1..2).map { |i| "stomp#{@protocol != "" ? "-#{@protocol}" : ""}://hastur-mq#{i}.#{domain}" }

    @stomp_client = OnStomp::Failover::Client.new 'failover:(#{clients.join(',')})'
  end

  def disconnect
    @stomp_client.disconnect
    @stomp_client = nil
  end

  def set_uuid(uuid)
    @uuid = uuid
    update_subscriptions
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
        @stomp_client.subscribe(topic, &block)
      else
        @stomp_client.subscribe(topic, @general_delivery_block)
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
    def receive_async(queue_name, &block)
      if block_given?
        @stomp_client.subscribe(topic, :ack => :client, &block)
      else
        @stomp_client.subscribe(topic, &@general_delivery_block)
      end
    end
  end

  module Direct
    def send_uuid(uuid, json_hash)
      @stomp_client.send(uuid, MultiJson.encode(json_hash))
    end
  end

  # This subscribes to direct messages and subscriptions for
  # general delivery.
  def subscribe_async(&block)
    @general_delivery_block = block

    update_subscriptions
  end

  def update_subscriptions
    @stomp_client.subscribe(@uuid, &@general_delivery_block) if @uuid && @general_delivery_block
  end

end
