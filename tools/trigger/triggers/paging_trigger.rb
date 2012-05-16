ctx = Hastur::Trigger::Context.new

# Guarantees about trigger behavior
#ctx.disable_stash!
#ctx.order_independent!

ctx.events(:attn => [ "tna-pager" ]) do |msg|
  puts "Paging TNA on event #{msg.name}!"
  pager_duty("TNA general page: #{msg.name}",
             (msg.subject rescue nil) || "No description",
             :message => msg.to_json, :uuid => msg.uuid,
             :hostname => msg.hostname)
end

# ctx.every(:minute) do
#   Hastur.gauge("load.spikes", ctx["total"])
# end
