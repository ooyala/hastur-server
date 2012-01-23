#
# Sends a message to Hastur every so often
#

require "singleton"
require_relative "hastur_messenger"

class HasturHeartbeat
  include Singleton

  attr_accessor :t

  #
  # Stops the heartbeat thread
  #
  def stop
    Thread.kill(@t)
  end

  #
  # Starts a heartbeat for the Hastur client
  #
  def start( interval )
    @t = Thread.start(interval) do |i|
      begin
        loop do
          HasturMessenger.instance.send( "{ \"method\" : \"heartbeat\", \"time\" : \"#{Time.now}\" }")
          sleep(interval)
        end
      rescue Exception => e
        STDERR.puts "Unable to send a heart message => #{e.message} \n\n#{e.backtrace}"
      end
    end
  end
end
