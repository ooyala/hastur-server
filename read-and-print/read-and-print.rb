require "rubygems"
require "hastur-mq"
require "pp"

if ARGS.empty?
  STDERR.puts "read-and-print [topics] [queues]"
  STDERR.puts "By default, assume topics.  Queues are prepended with q:"
  STDERR.puts "  Example: read-and-print errors q:notifications q:reliable-messages"
  exit
end

print_message = proc do |message|
  # TODO(noah): We could be snazzy here and get an actual mutex so we
  # never mangle printing when two packets arrive right
  # next to each other.
  pp message.inspect
end

ARGS.each do |topic|
  if topic =~ /^q:(.*)/
    HasturMq::Queue.subscribe(topic[2..-1], &print_message)
  else
    HasturMq::Topic.subscribe(topic, &print_message)
  end
end

# Loop forever, waiting
loop { }
