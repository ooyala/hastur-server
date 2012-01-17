require "#{File.dirname(__FILE__)}/../lib/hastur-mq"

c = HasturMq::Pull.new(["tcp://127.0.0.1:8001"], "topic")
consumer_thread = c.recv_async { |msg| puts msg }
puts "Listening for messages asynchronously..."

sleep 5
c.close

