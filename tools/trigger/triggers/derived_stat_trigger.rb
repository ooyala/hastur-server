ctx = Hastur::Trigger::Context.new

ctx.gauges(:name => "glowworm.latency") do |msg|
  puts "Received latency of #{msg.value} from Glowworm"
  ctx["latencies"] ||= []
  ctx["latencies"] << msg.value

  # An average is actually a terrible idea for latencies.  You're much
  # better off sending out, say, the best and worst latency for the
  # time period, and perhaps also the median.

  if ctx["latencies"].size > 10
    avg = ctx["latencies"].inject(0, &:+) / 10.0
    puts "Sending out 10-latency average of #{avg}"
    Hastur.gauge("glowworm.latency.moving_average", avg)
    ctx["latencies"] = []
  end
end
