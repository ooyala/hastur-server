#
# Starts a daemon that will monitor a machine. Statistics will be gathered
# by statsD and application plugins that will report numbers.
#
require "rubygems"
require "uuid"

# hastur libs
require "lib/json_builder"
require "lib/client_ports"
require "lib/hastur_heartbeats"
require "lib/hastur_logger"
require "lib/hastur_uuid_utils"

# receivers
require "receivers/hastur_message_receiver"

# hastur listeners
require "listeners/hastur_listener"

listeners = []

# message via MQ to register this machine with hastur
uuid = Hastur::Client::UuidUtils.instance.get_uuid
register_client_req = HasturJsonBuilder.get_register_client( uuid )
# prepare the messenger with our uuid so he knows what to tag messages as
HasturMessenger.instance.set_uuid( uuid )
# let Hastur know that the client is alive
HasturMessenger.instance.send(register_client_req)
HasturLogger.instance.log("Attempting to start up the client with uuid #{uuid}")

# listen on MQ for messages from Hastur
receiver = HasturMessageReceiver.new(HasturMessenger.instance.socket)
receiver.start

# listen for hastur traffic on a port
listeners << HasturListener.new(HasturClientConfig::HASTUR_PORT, :udp)
HasturLogger.instance.log("Listening on #{HasturClientConfig::HASTUR_PORT} for traffic")

# periodically give a client heartbeat
HasturHeartbeat.start( 30 )   # 30 second heartbeats

# block here until all of the threads die, WHICH SHOULD NEVER HAPPEN
listeners.each do |listener|
  listener.current_thread.join
  HasturLogger.instance.error( "Listener unexpectedly died => #{listener.name}" )
end

# TODO(viet): figure out how to properly handle when the code gets past here


