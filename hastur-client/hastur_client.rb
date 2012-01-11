#
# Starts a daemon that will monitor a machine. Statistics will be gathered
# by statsD and application plugins that will report numbers.
#
require "rubygems"
require "uuid"

# hastur libs
require "lib/json_builder"
require "lib/client_ports"

# hastur listeners
require "listeners/service_listener"

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

# TODO(viet): listen for statsd traffic

# TODO(viet): listen for service registration traffic
listeners << HasturServiceListener.new(HasturClientConfig::SERVICE_REGISTRATION_PORT, :tcp)

# TODO(viet): listen for plugin registration traffic

# TODO(viet); listen for alert traffic


# block here until all of the threads die, WHICH SHOULD NEVER HAPPEN
listeners.each do |listener|
  listener.current_thread.join
  STDERR.puts "Listener unexpectedly died => #{listener.name}"
end

# TODO(viet): figure out how to properly handle when the code gets past here


