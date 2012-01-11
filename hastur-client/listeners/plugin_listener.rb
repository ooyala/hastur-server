#
# A listener that processes REGISTER_PLUGIN events
#

require "#{File.dirname(__FILE__)}/hastur_listener"

class HasturPluginListener < HasturListener
  def initialize(port, type)
    super(port, type)
  end

  def process_message(msg)
    begin
      msg = JSON.parse(msg)
      # TODO(viet): put this message on the STOMP mq
    rescue Exception => e
      STDERR.puts "Unabled to process the message."
      STDERR.puts e.message
    end
  end
end
