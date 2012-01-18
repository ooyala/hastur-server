require "#{File.dirname(__FILE__)}/../lib/hastur-mq"

link = "tcp://127.0.0.1:8001"

p = HasturMq::Dealer.new(link, "ThisIsMyVersionOfAUuid")

##########################################################################
# Continuously sends information
##########################################################################
send_thr = Thread.start do
  begin
    1000.times do |i|
      p.send("{ 'msg' : '#{i}' }")
      sleep 1
    end
  rescue Exception => e
    puts e.message
  end
end

r = ZMQ::Context.new.socket(ZMQ::ROUTER)
r.bind( link )
loop do
  r.recv_string(msg="")
  puts "router => #{msg}"
end


