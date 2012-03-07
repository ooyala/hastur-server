$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..")

require "base_sink"
require "hastur-server/zmq_utils"

#
# Event sink will drain several routers' sockets and push the event
# messages into Cassandra. Upon a successful Cassandra write, it will send an
# event ack message acknowledging that the event was successfully
# stored to disk.
#
class EventSink < Hastur::Sink
 
  def initialize
    super
    @to_socket = Hastur::ZMQUtils.connect_socket(@context, ::ZMQ::PUSH, @opts[:routers].flatten)
  end

  #
  # Starts the receiving the processing of data on the connected URIs.
  #
  def start
    @running = true
    while @running do
      message = Hastur::Message.recv(@socket)
      uuid = message.envelope.from
      Hastur::Cassandra.insert(@client, message.payload, "event", :uuid => uuid)
      # send an event ack
      message.envelope.to_ack.send( @to_socket )
    end
  end
end

