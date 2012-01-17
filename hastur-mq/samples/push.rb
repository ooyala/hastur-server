require "#{File.dirname(__FILE__)}/../lib/hastur-mq"

p = HasturMq::Push.new("tcp://127.0.0.1:8001")

puts "Preparing to send messages..."
loop do
  puts p.send("{ 'msg' : 'helllllllo' }")
  sleep 1
end

