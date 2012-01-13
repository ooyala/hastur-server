#!/usr/bin/env ruby

require "rubygems"
require "multi_json"
require "hastur-mq"

if ARGV.size != 2
  STDERR.puts "Usage: #{$0} <queue_or_topic> <filename>"
  STDERR.puts "  The file should contain an array of JSON objects to send."
  STDERR.puts "  Queue_or_topic should be a topic name, or a queue name with 'q:' in front."
  STDERR.puts "Examples:"
  STDERR.puts "  #{$0} transient_messages ./replay_file.json"
  STDERR.puts "  #{$0} q:notifications notifications_to_send.json"
  exit
end

input = MultiJson.decode(File.read ARGV[1])
raise "Illegal JSON in file #{ARGV[1]}!" unless input.kind_of?(Array) || input.kind_of?(Hash)

input = [input] if input.kind_of?(Hash)

HasturMq.connect

# Get queue or topic name
is_queue? = false
name = ARGV[0]
if ARGV[0] =~ /^q:/
  is_queue? = true
  name = ARGV[0][2..-1]
end

input.each do |json_obj|
  if is_queue?
    HasturMq::Queue.send(name, json_obj)
  else
    HasturMq::Topic.send(name, json_obj)
  end
end
