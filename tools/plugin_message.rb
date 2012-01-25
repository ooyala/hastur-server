require "ffi-rzmq"

if ARGV.length != 1
  raise "Usage: ruby plugin_message.rb <client_uuid>"
end

ctx = ZMQ::Context.new
router = ctx.socket(ZMQ::ROUTER)
router.bind("tcp://127.0.0.1:4321")

sleep 1

1.times do |i|
  # TODO(viet): this identity is specific to Viet's laptop. Figure out how to get it dynamic? Not sure if this is really worth it.
  router.send_string(ARGV[0], ZMQ::SNDMORE)
  router.send_string("schedule", ZMQ::SNDMORE)
  router.send_string('{ "name" : "basic", "path" : "/Users/viet/work/repos/hastur/sample-plugins/basic.rb"}')
end
