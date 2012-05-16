ctx = Hastur::Trigger::Context.new

def server_record(ctx, msg)
  ctx["servers"] ||= {}
  ctx["servers"][msg.uuid] ||= { "hostname" => msg.hostname }

  server = ctx["servers"][msg.uuid]

  server
end

ctx.events(:name => "glowworm.exception") do |msg|
  puts "Glowworm got a server error"

  server = server_record(ctx, msg)

  server["exceptions"] ||= 0
  server["exceptions"] += 1
end

ctx.counters(:name => "glowworm.fetch.fresh") do |msg|
  server = server_record(ctx, msg)

  server["fresh"] ||= 0
  server["fresh"] += 1

  server["total"] ||= 0
  server["total"] += 1
end

ctx.counters(:name => "glowworm.fetch.thread.existing") do |msg|
  server = server_record(ctx, msg)

  server["join_thread"] ||= 0
  server["join_thread"] += 1

  server["total"] ||= 0
  server["total"] += 1
end

ctx.counters(:name => "glowworm.fetch.thread.new") do |msg|
  server = server_record(ctx, msg)

  server["new_thread"] ||= 0
  server["new_thread"] += 1

  server["total"] ||= 0
  server["total"] += 1
end

ctx.counters(:name => "glowworm.feature.return.no_cache") do |msg|
  puts "Glowworm returned made-up data on a non-prefetch"

  server = server_record(ctx, msg)

  server["no_cache"] ||= 0
  server["no_cache"] += 1
end

ctx.counters(:name => "glowworm.fetch.timeout") do |msg|
  return unless msg.labels["timeout"] && msg.labels["timeout"] > 0.4999

  puts "Glowworm timed out waiting for server for at least 0.5 seconds"

  server = server_record(ctx, msg)

  server["timeouts"] ||= 0
  server["timeouts"] += 1
end

def gw_exception_email(uuid, hash)
  send_email "noah@ooyala.com",
    "Glowworm on #{hash["hostname"]} got too many exceptions",
    <<BODY,
Glowworm on #{hash["hostname"]} (UUID #{uuid}) got too many exceptions.

Specifically, it got #{hash["exceptions"]} on #{hash["total"]} total requests.

Summary data:
#{hash.inspect}
BODY
    :cc => "tna-team@ooyala.com"
end

def gw_summary!(ctx)
  ctx["servers"] ||= {}

  ctx["server"].each do |uuid, hash|
    if hash["exceptions"] >= hash["total"] ||
        hash["exceptions"] > ((hash["total"] / 20.0) + 2.0)
      gw_exception_email uuid, hash
    end
  end

  ctx["servers"] = {}
end

ctx.every(:day) do
  gw_summary!
end

ctx.events(:name => "glowworm.trigger.reset") do |msg|
  gw_summary!
end
