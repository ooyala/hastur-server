#
# This is a base class for all HasturMessageProcessors. It contains shared functionality needed
# to process all messages.
#
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
  def flush_to_hastur(msg)
    # TODO(viet): implement this once the STOMP wrapper is available
    STDOUT.puts "Pretending to send => #{msg}"
  end
end
