# This is a prototype for the message bus interface for the new Ooyala
# monitoring system being designed by the Tools and Automation Team.

# This interface is currently only asynchronous.  Want synchronous?
# Use "t = Thread.new { async_method { /* handler */ }; sleep 10000 }; t.join(SECONDS_TO_WAIT) || t.kill".
#

require "onstomp"
require "multi_json"
require "hastur-mq/version"

module HasturMq
  def self.stomp_client
    @stomp_client
  end

  def self.connect(options = {})
    conf_file = File.read("/etc/hastur.json") rescue ""
    @hastur_settings = MultiJson.decode(conf_file)
    @hastur_settings ||= {}
    @domain = @hastur_settings["domain"] || "us-west-2.ooyala.com"

    @hostname = @hastur_settings["hostname"] || `hostname`
    @protocol = @hastur_settings["protocol"] || ""

    # TODO(noah): Find UUID?  Or require it be set?  Ignore until we support direct messages.

    #clients = (1..2).map { |i| "stomp#{@protocol != "" ? "-#{@protocol}" : ""}://hastur-mq#{i}.#{@domain}" }
    #client_default = "failover:(#{clients.join(',')})"
    client_url = @hastur_settings["client-url"] || ENV["HASTUR_URL"] || options["stomp-server-url"] || "stomp://localhost"

    #@stomp_client = OnStomp::Failover::Client.new client_url
    STDERR.puts "Connecting to URL #{client_url}"
    @stomp_client = OnStomp::Client.new client_url
    @stomp_client.host = "/"
    @stomp_client.login = "guest"
    @stomp_client.passcode = "guest"
    @stomp_client.connect
  end

  def self.disconnect
    @stomp_client.disconnect
    @stomp_client = nil
  end

  def self.set_uuid(uuid)
    @uuid = uuid
    update_subscriptions
  end

  module Topic
    def self.send(topic, json_hash)
      HasturMq.stomp_client.send("/topic/" + topic, MultiJson.encode(json_hash))
    end

    # This subscribes asynchronously to only this
    # topic.  Without a block, subscribe for
    # general delivery.
    def self.receive_async(topic, &block)
      if block_given?
        HasturMq.stomp_client.subscribe("/topic/" + topic, &block)
      else
        raise "No general delivery without a general delivery subscription!" unless @general_delivery_block
        HasturMq.stomp_client.subscribe("/topic/" + topic, @general_delivery_block)
      end
    end
  end

  module Queue
    def self.send(queue_name, json_hash)
      HasturMq.stomp_client.send("/queue/" + queue_name, MultiJson.encode(json_hash))
    end

    # This subscribes asynchronously to only this
    # queue.  Without a block, subscribe for
    # general delivery.
    def self.receive_async(queue_name, &block)
      if block_given?
        HasturMq.stomp_client.subscribe("/queue/" + queue_name, :ack => :client, &block)
      else
        raise "No general delivery without a general delivery subscription!" unless @general_delivery_block
        HasturMq.stomp_client.subscribe("/queue/" + queue_name, &@general_delivery_block)
      end
    end
  end

  module Direct
    def self.send_uuid(uuid, json_hash)
      raise "No Direct messages supported yet!"
    end
  end

  # This subscribes to direct messages and subscriptions for
  # general delivery.
  def self.subscribe_async(&block)
    @general_delivery_block = block

    update_subscriptions
  end

  def self.update_subscriptions
    # Eventually this will subscribe usefully for direct messages
    #@stomp_client.subscribe(@uuid, &@general_delivery_block) if @uuid && @general_delivery_block
  end

end
