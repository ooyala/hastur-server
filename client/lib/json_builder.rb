require "#{File.dirname(__FILE__)}/machine_info"

#
# Provides a set of utility functions to build well-formed JSON requests
# which Hastur will be able to understand.
#
module HasturJsonBuilder

  REGISTER_CLIENT="register_client"

  #
  # Builds a JSON request that will register this client with Hastur
  #
  def self.get_register_client
    msg = Hash.new
    msg["params"] = MachineInfo.get_machine_info
    msg["id"] = UUID.new.generate
    msg["method"] = HasturJsonBuilder::REGISTER_CLIENT
    msg.to_json
  end
end
