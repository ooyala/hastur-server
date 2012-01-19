#
# Starts a daemon that will monitor a machine. Statistics will be gathered
# by statsD and application plugins that will report numbers.
#
require "rubygems"
require "uuid"

# hastur libs
require "lib/json_builder"
require "lib/client_ports"
require "lib/hastur_heartbeats.rb"
require "lib/hastur_logger"

# receivers
require "receivers/hastur_message_receiver"

# hastur listeners
require "listeners/hastur_listener"

#
# Saves the Hastur client UUID in the current location under .hastur_client_uuid
#
def save_uuid( uuid, filepath )
  unless File.exists?( filepath )
    # create the file with the uuid
    File.open(filepath, 'w') {|f| f.write( uuid ) }
  end
end

#
# Retrieves the UUID from the current location under .hastur_client_uuid
# if the file exists. Otherwise return a newly generated UUID.
#
def get_uuid
  # TODO(viet): figure out how to better deal with the UUID
  filepath = "#{File.dirname(__FILE__)}/.hastur_client_uuid"
  uuid = nil
  if File.exists?( filepath )
    # read from file to get the UUID
    f = File.new( filepath, "r")
    uuid = f.gets.chomp
  else
    # generate a new UUID and save it
    uuid = UUID.new.generate
    save_uuid( uuid, filepath )
  end
  uuid
end

listeners = []


# message via MQ to register this machine with hastur
uuid = get_uuid()
register_client_req = HasturJsonBuilder.get_register_client( get_uuid() )
HasturMessenger.instance.set_uuid( uuid )
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


