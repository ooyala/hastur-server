require_relative "../test_helper"
require "minitest/autorun"
require "rack-test"
require "hastur-server/service/retrieval"

class RetrievalServiceTest < MiniTest::Unit::TestCase
  include Rack::Test::Methods

  def app
    Hastur::Service::Retrieval.new
  end

  def test_blah
    get "/api"
    # follow_redirect!

    assert last_response.ok?, "Request must succeed"
    assert MultiJson.load(last_response.body).keys.size == 5, "Must have five keys"
  end
end
