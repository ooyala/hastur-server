#
# Bubbles any hastur-client logs up to Hastur
#

require "singleton"
require_relative "../msg_processors/message_processor"
require_relative "hastur_messenger"

class HasturLogger
  include Singleton
  
  #
  # Allows the logger to add additional information to the log message before sending
  # it off to the Hastur server.
  #
  def log(msg)
    HasturMessenger.instance.send(msg)
  end

  #
  # Allows the logger to add additional information to the error message before sending
  # it off to the Hastur server.
  #
  def error(msg)
    HasturMessenger.instance.send( msg)
  end
end
