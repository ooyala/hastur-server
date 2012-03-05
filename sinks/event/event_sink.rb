$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")

require "base_sink"

#
# Event sink will drain several routers' sockets and push the event
# messages into Cassandra. Upon a successful Cassandra write, it will send an
# event ack message acknowledging that the event was successfully
# stored to disk.
#
class EventSink < Hastur::Sink
  
  #
  # Starts the receiving the processing of data on the connected URIs.
  #
  def start
    while @running do
      message = Hastur::Message.recv(@socket)
      uuid = message.envelope.from
      if Hastur::Cassandra.insert(@client, message.payload, "event", :uuid => uuid)
        # TODO(viet): send an event ack
      else
        # TODO(viet); log an error complaining that the event was not successfully persisted
      end
    end
  end
end

