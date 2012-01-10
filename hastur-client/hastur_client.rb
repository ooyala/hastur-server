#
# Starts a daemon that will monitor a machine. Statistics will be gathered
# by statsD and application plugins that will report numbers.
#
require "rubygems"
require "lib/json_builder"

plugins = []

# TODO(viet): message via STOMP to register this machine with hastur
register_client_req = HasturJsonBuilder.get_register_client

# TODO(viet): listen on STOMP topic for scheduled plugin execution




