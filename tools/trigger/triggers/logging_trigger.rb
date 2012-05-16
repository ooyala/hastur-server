ctx = Hastur::Trigger::Context.new

STDOUT.sync = true

ctx.gauges(:name => "write.out.gauge") do |msg|
  print "MSG: gauge #{msg.name.inspect} #{msg.value.inspect}\n"
end

ctx.counters(:name => "write.out.counter") do |msg|
  print "MSG: counter #{msg.name.inspect} #{msg.value.inspect}\n"
end

ctx.marks(:name => "write.out.mark") do |msg|
  print "MSG: mark #{msg.name.inspect} #{msg.value.inspect}\n"
end

ctx.events(:name => "write.out.event") do |msg|
  print "MSG: event #{msg.name.inspect}\n"
end

#ctx.hb_no_such_thing(:name => "bob") do |msg|
#  print "MSG: event #{msg.name.inspect} #{msg.value.inspect}\n"
#end

ctx.hb_processes(:name => "write.out.hb_process") do |msg|
  print "MSG: hb_process #{msg.name.inspect} #{msg.value.inspect}\n"
end
