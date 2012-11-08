require "hastur-server/service/retrieval"
require "hastur-rack"

# defined on most of our Hastur boxes via hastur-deploy
cassandra_servers = []

if ENV['CASSANDRA_URIS']
  cassandra_servers = MultiJson.load(ENV['CASSANDRA_URIS']).flatten
else
  cassandra_servers = [ '127.0.0.1:9160' ]
end

use Hastur::Rack, "hastur.retrieval"

run Hastur::Service::Retrieval.new cassandra_servers
