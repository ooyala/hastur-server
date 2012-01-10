#
# Starts a daemon that will monitor a machine. Statistics will be gathered
# by statsD and application plugins that will report numbers.
#
require "rubygems"
require "lib/json_builder"
require "uuid"

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

# TODO(viet): message via STOMP to register this machine with hastur
register_client_req = HasturJsonBuilder.get_register_client( get_uuid() )

# TODO(viet): listen on STOMP topic for scheduled plugin execution





