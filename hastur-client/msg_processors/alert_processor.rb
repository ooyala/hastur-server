#
# The HasturAlertProcessor will register a service with Hastur.
#

require "#{File.dirname(__FILE__)}/message_processor"

class HasturAlertProcessor < HasturMessageProcessor
  
  ALERT="alert"
  
  def initialize
    super( ALERT )
  end

  #
  # Checks if the message is a ALERT type and processes if true
  #
  def process_message(msg)
    if msg["method"] == @method
      STDOUT.puts "Received a #{@method} request => #{msg}"
      # TODO(viet): place this message on STOMP

      return true
    end
    return false
  end
end
