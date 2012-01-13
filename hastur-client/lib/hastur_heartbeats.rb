#
# Sends a message to Hastur every so often
#

require "#{File.dirname(__FILE__)}/hastur_messenger"

class HasturHeartbeat

  HEARTBEAT_QUEUE="/queue/hastur/heartbeat"

  #
  # Starts a heartbeat for the Hastur client
  #
  def self.start( interval )
    t = Thread.start(interval) do |i|
      loop do
        HasturMessenger.instance.send( HEARTBEAT_QUEUE, "{ 'method' => 'heartbeat', 'time' => '#{Time.now}' }")
        sleep(interval)
      end
    end
  end
end
