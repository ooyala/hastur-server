$LOAD_PATH.unshift File.dirname(__FILE__)

require "hastur-dashboard"

RETRIEVAL_SERVICE_URI = "127.0.0.1:9000"

run Rack::URLMap.new("/" => Hastur::Flot::Dashboard.new(RETRIEVAL_SERVICE_URI))
