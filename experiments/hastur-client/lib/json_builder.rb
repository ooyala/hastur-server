require_relative "machine_info"

#
# Provides a set of utility functions to build well-formed JSON requests
# which Hastur will be able to understand.
#
module HasturJsonBuilder

  REGISTER_CLIENT="register_client"
  REGISTER_SERVICE="register_service"
  REGISTER_PLUGIN="register_plugin"
  NOTIFICATION="notify"

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
  #     lifetime - the length of time this service should live for. Unlimited lifetime is -1.
  #     uuid - the hastur client's UUID
  #
  def self.get_register_service(name, plugin, lifetime, uuid)
    # set up the service parameters
    params = Hash.new
    params["name"] = name
    params["plugin"] = plugin
    params["plugin_params"] = plugin_params
    params["lifetime"] = lifetime
    params["client_id"] = uuid
    # set up the JSON req
    msg = Hash.new
    msg["params"] = params
    msg["method"] = REGISTER_SERVICE
    msg.to_json
  end

  # 
  # Builds a JSON request that will register a plugin with Hastur
  #
  # Params:
  #    name - Human readable name for the plugin
  #    path - filepath to the executable plugin
  #    lifetime - the length of time this plugin should live for. Unlimited lifetime is -1.
  #    uuid - the hastur client's UUID
  #
  def self.get_register_plugin(name, path, lifetime, uuid)
    params = Hash.new
    params["client_id"] = uuid
    params["name"] = name
    params["path"] = path
    params["lifetime"] = lifetime
    # set up the JSON req
    msg = Hash.new
    msg["params"] = params
    msg["method"] = REGISTER_PLUGIN
    msg.to_json
  end

  #
  # Builds a JSON request that will send Hastur an alert/notification.
  #
  # Params:
  #     name - Alert/notification message
  #     subsystem - ?
  #     uuid - the hastur client's UUID
  #
  def self.get_alert(name, subsystem, uuid)
    params = Hash.new
    params["client_id"] = uuid
    params["name"] = name
    params["subsystem"] = subsystem
    msg = Hash.new
    msg["params"] = params
    msg["method"] = NOTIFICATION
    msg.to_json
  end
end
