$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

#
# to run against the production cluster, fire up an ssh port forward
# ssh -L 9202:localhost:9202 hastur-core1.us-east-1.ooyala.com
#

require "cassandra/1.0"
require "hastur-server/service/retrieval"

ENV['CASSANDRA_URIS'] = '["127.0.0.1:9202"]'

run Rack::URLMap.new("/" => Hastur::Service::Retrieval.new)
