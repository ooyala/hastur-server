#
# Sends a message to Hastur every so often
#
module HasturHeartbeat

  #
  # Starts a heartbeat for the Hastur client
  #
  def self.start( interval )
    t = Thread.start(interval) do |i|
      loop do
        # TODO(viet): send a heartbeat message to Hastur

        STDOUT.puts "{ 'method' => 'heartbeat', 'time' => '#{Time.now}' }"
        sleep(interval)
      end
    end
  end
end
