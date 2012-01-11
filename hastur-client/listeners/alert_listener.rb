#
# A listener that processes ALERT events
#

require "#{File.dirname(__FILE__)}/hastur_listener"

class HasturAlertListener < HasturListener
  def initialize(port, type)
    super(port, type)
  end

  #
  # Processes an alert message
  #
  def process_message(msg)
    begin
      msg = JSON.parse(msg)
      # TODO(viet): put this message on the STOMP mq

    rescue Exception => e
      STDERR.puts "Unable to process message."
      STDERR.puts e.message
    end
  end
end
