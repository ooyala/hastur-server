#!/opt/local/bin/ruby

#
# This script will start the hastur client.
#

require_relative "lib/hastur_logger"
require_relative "hastur_client"

client = HasturClient.new
client.start

client.listeners.each do |listener|
  listener.current_thread.join
  HasturLogger.instance.error("Listener unexpectedly died => #{listener.name}")
end

# TODO(viet): figure out how to properly handle when the code gets to this point
