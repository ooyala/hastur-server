#
# Sends a message to Hastur every so often
#

require "#{File.dirname(__FILE__)}/hastur_messenger"

class HasturHeartbeat

  #
  # Starts a heartbeat for the Hastur client
  #
  def self.start( interval )
    t = Thread.start(interval) do |i|
      begin
        loop do
          HasturMessenger.instance.send( "{ 'method' => 'heartbeat', 'time' => '#{Time.now}' }")
          sleep(interval)
        end
      rescue Exception => e
        STDERR.puts "Unable to send a heart message => #{e.message} \n\n#{e.backtrace}"
      end
    end
  end
end
