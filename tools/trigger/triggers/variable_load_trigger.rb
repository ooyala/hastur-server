ctx = Hastur::Trigger::Context.new

# Guarantees about trigger behavior
#ctx.disable_stash!
#ctx.order_independent!

ctx.gauges(:name => "collectd.load") do |msg|
  if msg.value > 10.0
    # PagerDuty requires an incident ID, a message, and has an optional
    # JSON hash of extra stuff.  Pass in the message automatically?  Or
    # just its UUID and timestamp?
    pager_duty("Monitoring-load-spiking-#{msg.uuid}",
               "The load has spiked to #{msg.value} on host #{msg.hostname}",
               :message => msg.to_json, :load => msg.value, :uuid => msg.uuid,
               :hostname => msg.hostname)

    ctx["total"] ||= 0
    ctx["total"] += 1

    puts "VLT: Received high-value message!"
  end
end

ctx.events(:name => "load.reset") do |msg|
  ctx["total"] = 0
  puts "VLT: Received reset event!"
end

# ctx.every(:minute) do
#   Hastur.gauge("load.spikes", ctx["total"])
# end
