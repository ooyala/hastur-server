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

# TODO(viet): message via STOMP to register this machine with hastur
register_client_req = HasturJsonBuilder.get_register_client( get_uuid() )

# TODO(viet): listen on STOMP topic for scheduled plugin execution

# listen for hastur traffic on a port
listeners << HasturListener.new(HasturClientConfig::HASTUR_PORT, :udp)

# periodically give a client heartbeat
HasturHeartbeat.start( 30 )   # 30 second heartbeats

# block here until all of the threads die, WHICH SHOULD NEVER HAPPEN
listeners.each do |listener|
  listener.current_thread.join
  HasturErrorProcessor.instance.log( "Listener unexpectedly died => #{listener.name}" )
end

# TODO(viet): figure out how to properly handle when the code gets past here


