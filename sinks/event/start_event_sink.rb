#!/usr/bin/env ruby
$LOAD_PATH.unshift File.dirname(__FILE__)

require "event_sink"
begin
  sink = EventSink.new
  sink.start
rescue Exception => e
  puts e.message
  puts e.backtrace
end
