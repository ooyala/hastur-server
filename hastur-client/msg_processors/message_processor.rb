#
# This is a base class for all HasturMessageProcessors. It contains shared functionality needed
# to process all messages.
#
class HasturMessageProcessor
  attr_accessor :method
  def initialize(method)
    @method = method
  end

  def process_message(msg)
    raise "This feature is unimplemented."
  end
end
