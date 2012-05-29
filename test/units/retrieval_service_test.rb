require_relative "../test_helper"
require "minitest/autorun"
require "rack/test"
require "hastur-server/service/retrieval"

class RetrievalServiceTest < MiniTest::Unit::TestCase
  include Rack::Test::Methods

  def app
    # Supply no URIs for Cassandra
    Hastur::Service::Retrieval.new []
  end

  def get_response_hash(uri, options = {})
    get uri
    #follow_redirect!

    assert last_response.ok?, "Request to #{uri} must succeed"
    MultiJson.load(last_response.body)
  end

  def test_top_level
    hash = get_response_hash "/api"

    assert hash.keys.include?("node"), "Must contain Node URL"
  end

  def test_types
    hash = get_response_hash "/api/type"

    assert hash.keys.include?("all"), "Types hash must include 'all'"
    assert hash.keys.include?("stat"), "Types hash must include 'stat'"
    assert hash.keys.include?("event"), "Types hash must include 'event'"
  end
end
