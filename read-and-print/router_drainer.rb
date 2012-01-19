require "ffi-rzmq"

ctx = ZMQ::Context.new
router = ctx.socket(ZMQ::ROUTER)
router.bind("tcp://127.0.0.1:8000")

poller = ZMQ::Poller.new
poller.register(router, ZMQ::POLLIN)
msg = ZMQ::Message.new

loop do
  poller.poll(1)
  poller.readables.each do |s|
    msgs = []
    loop do
      s.recv( msg )
      has_more = s.more_parts?
      msgs << msg.copy_out_string
      puts "drainer => #{msgs[-1]}"
      break unless has_more
    end
    puts ""
    router.send_string(msgs[0], ZMQ::SNDMORE)
    router.send_string(msgs[1])
  end
end
