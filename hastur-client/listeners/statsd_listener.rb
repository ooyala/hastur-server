#
# A listener that processes STATSD events
#

require "#{File.dirname(__FILE__)}/hastur_listener"

class HasturStatsDListener < HasturListener
  def initialize(port, type)
    super(port, type)
  end

  #
  # Processes a statsd event.
  #
  def process_message(msg)
    begin
      msg = JSON.parse(msg)
      # TODO(viet): put this message on the STOMP mq
    rescue Exception => e
      STDERR.puts "Unable to process the message."
      STDERR.puts e.message
    end
  end
end
