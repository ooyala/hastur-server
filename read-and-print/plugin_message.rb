require "ffi-rzmq"

ctx = ZMQ::Context.new
router = ctx.socket(ZMQ::ROUTER)
router.bind("tcp://127.0.0.1:8000")

sleep 1

1.times do |i|
  # TODO(viet): this identity is specific to Viet's laptop. Figure out how to get it dynamic? Not sure if this is really worth it.
  router.send_string("b967bd00-1de7-012f-13c6-109addba6b5d", ZMQ::SNDMORE)
  router.send_string("execute_plugin", ZMQ::SNDMORE)
  router.send_string('{ "name" : "basic", "path" : "/Users/viet/work/repos/hastur/sample-plugins/basic.rb"}')
end
