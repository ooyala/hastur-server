#
# The HasturStatsDProcessor will register a service with Hastur.
#

require "#{File.dirname(__FILE__)}/message_processor"

class HasturStatsdProcessor < HasturMessageProcessor
  
  STATSD="statsd"
  STATS_TOPIC="/topic/hastur/stats"

  def initialize
    super( STATSD )
  end

  #
  # Checks if the message is a STATSD type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      flush_to_hastur( STATS_TOPIC, msg )
      return true
    end
    return false
  end
end
