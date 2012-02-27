$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")

require "base_sink"

#
# Notification sink will drain several routers' sockets and push the notification
# messages into Cassandra. Upon a successful Cassandra write, it will send a 
# notification ack message acknowledging that the notification was successfully
# stored to disk.
#
class NotificationSink < Hastur::Sink
  
  #
  # Starts the receiving the processing of data on the connected URIs.
  #
  def start
    while @running do
      message = Hastur::Message.recv(@socket)
      uuid = message.envelope.from
      if Hastur::Cassandra.insert(@client, message.payload, :uuid => uuid, :route => "notification")
        # TODO(viet): send a notification ack
      else
        # TODO(viet); log an error complaining that the notification was not successfully persisted
      end
    end
  end
end

