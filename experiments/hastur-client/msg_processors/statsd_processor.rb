#
# The HasturStatsDProcessor will register a service with Hastur.
#

require_relative "../lib/client_config"
require_relative "message_processor"

class HasturStatsdProcessor < HasturMessageProcessor
  
  STATSD="statsd"

  def initialize
    super( STATSD )
  end

  #
  # Checks if the message is a STATSD type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur(HasturClientConfig::STATS_ROUTE, msg.to_json)
      return true
    end
    return false
  end
end
