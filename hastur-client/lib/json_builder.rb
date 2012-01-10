require "#{File.dirname(__FILE__)}/machine_info"

#
# Provides a set of utility functions to build well-formed JSON requests
# which Hastur will be able to understand.
#
module HasturJsonBuilder

  REGISTER_CLIENT="register_client"
  REGISTER_SERVICE="register_service"

  #
  # Builds a JSON request that will register this client with Hastur
  #
  def self.get_register_client( uuid )
    msg = Hash.new
    msg["params"] = MachineInfo.get_machine_info( uuid )
    msg["id"] = UUID.new.generate
    msg["method"] = HasturJsonBuilder::REGISTER_CLIENT
    msg.to_json
  end

  #
  # Builds a JSON request that will register a service with Hastur
  # 
  # Params:
  #     name - Human readable name for the service
  #     plugin - unique key for the plugin
  #     plugin_params
  #
  def self.get_register_service(name, plugin, plugin_params, lifetime)
    # set up the service parameters
    params = Hash.new
    params["name"] = name
    params["plugin"] = plugin
    params["plugin_params"] = plugin_params
    params["lifetime"] = lifetime
    # set up the JSON req
    msg = Hash.new
    msg["params"] = params
    msg["method"] = REGISTER_SERVICE
    msg.to_json
  end
end
