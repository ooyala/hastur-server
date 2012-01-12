#!/usr/bin/env ruby

require "rubygems"
require "hastur-mq"
require "pp"

if ARGV.empty?
  STDERR.puts "#{$0} [topics] [queues]"
  STDERR.puts "By default, assume topics.  Queues are prepended with q:"
  STDERR.puts "  Example: read-and-print errors q:notifications q:reliable-messages"
  exit
end

# TODO(noah): pass the RabbitMQ URL in some saner way
ENV['HASTUR_URL'] = "localhost"
HasturMq.connect

print_message = proc do |message|
  # TODO(noah): We could be snazzy here and get an actual mutex so we
  # never mangle printing when two packets arrive right
  # next to each other.
  pp message.inspect
end

ARGV.each do |topic|
  if topic =~ /^q:(.*)/
    HasturMq::Queue.receive_async(topic[2..-1], &print_message)
  else
    HasturMq::Topic.receive_async(topic, &print_message)
  end
end

# Loop forever, waiting
loop { }
