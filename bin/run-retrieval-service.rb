#!/usr/bin/env ruby
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "web")

require "rack"
require "hastur-retrieval-service"

CASSANDRA_URIS = ["127.0.0.1:9160"]

Rack::Handler::WEBrick.run(
  Hastur::RetrievalApp.new( CASSANDRA_URIS ),
  :Port => 9000
)
