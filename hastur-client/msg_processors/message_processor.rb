#
# This is a base class for all HasturMessageProcessors. It contains shared functionality needed
# to process all messages.
#

require "#{File.dirname(__FILE__)}/../lib/hastur_messenger"

class HasturMessageProcessor
  attr_accessor :method
  def initialize(method)
    @method = method
  end

  #
  # Undefined stub to process an incoming message. Subclasses should be overriding
  # this method.
  #
  def process_message(msg)
    raise "This feature is unimplemented."
  end

  #
  # Sends a message to STOMP for Hastur to pick up
  #
  def flush_to_hastur(topic_name, msg)
    HasturMessenger.instance.send(topic_name, msg)
  end
end
