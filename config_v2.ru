# When deploying as a war, the hastur-server gem itself isn't installed as a gem.
$LOAD_PATH << File.join(File.dirname(__FILE__), "lib")

require "hastur-server/api/v2"
require "hastur-rack"

# defined on most of our Hastur boxes via hastur-deploy
cassandra_servers = []

if ENV['CASSANDRA_URIS']
  cassandra_servers = MultiJson.load(ENV['CASSANDRA_URIS']).flatten
else
  cass_port = ENV["HASTUR_CASS_PORT"] || "9160"
  cassandra_servers = [ "127.0.0.1:#{cass_port}" ]
end

use Hastur::Rack, "hastur.retrieval"

run Hastur::Service::RetrievalV2.new cassandra_servers
