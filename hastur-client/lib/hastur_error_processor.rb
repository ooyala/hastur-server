#
# Bubbles any hastur-client errors up to Hastur
#

require "singleton"
require "#{File.dirname(__FILE__)}/../msg_processors/message_processor"

class HasturErrorProcessor < HasturMessageProcessor
  include Singleton

  ERROR_MSG="error_msg"
  
  def initialize
    super( ERROR_MSG )
  end

  #
  # Wrapper method around HasturMessageProcessor.flush_to_hastur(). Allows
  # the processor to add additional information to the message before sending
  # it off to the Hastur server.
  #
  def log(msg)
    flush_to_hastur("Error: #{msg}")
  end
end
