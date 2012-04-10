$LOAD_PATH.unshift File.dirname(__FILE__)

require "hastur-retrieval-service"

CASSANDRA_URIS = ["127.0.0.1:9160"]

run Rack::URLMap.new("/" => Hastur::RetrievalApp.new(CASSANDRA_URIS))
