ctx = Hastur::Trigger::Context.new

ctx.events(:attn => ["noah@ooyala.com"]) do |msg|
  STDERR.puts "Email for Noah!  OMG!"
  send_email("noah@ooyala.com", "OMG! #{msg.subject}",
             "A message totally showed up for you.  It said:\n#{msg.to_json}",
             :from => "omg!authority@hastur.ooyala.com")
end
