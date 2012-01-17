require "#{File.dirname(__FILE__)}/../lib/hastur-mq"

p = HasturMq::Publisher.new("tcp://127.0.0.1:8001")

loop do
  puts p.send("topic", "{ 'msg' : 'helllllllo' }")
  sleep 1
end

