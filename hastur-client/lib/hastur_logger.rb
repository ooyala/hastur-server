#
# Bubbles any hastur-client logs up to Hastur
#

require_relative "../lib/client_config"
require_relative "../msg_processors/message_processor"
require_relative "hastur_messenger"

class HasturLogger
  
  class << self

    #
    # Allows the logger to add additional information to the log message before sending
    # it off to the Hastur server.
    #
    def log(msg)
      HasturMessenger.send(HasturClientConfig::LOG_ROUTE, msg)
    end

    #
    # Allows the logger to add additional information to the error message before sending
    # it off to the Hastur server.
    #
    def error(msg)
      HasturMessenger.send(HasturClientConfig::ERROR_ROUTE, msg)
    end

  end
end
