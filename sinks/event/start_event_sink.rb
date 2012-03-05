#!/usr/bin/env ruby
$LOAD_PATH.unshift File.dirname(__FILE__)

require "event_sink"

sink = EventSink.new
sink.start
