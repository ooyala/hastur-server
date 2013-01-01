require_relative "../test_helper"
require "rack/test"

require "hastur-server/api/v2"
require "hastur-server/api/cass_java_client"

class RetrievalServerTest < Scope::TestCase
  include Rack::Test::Methods

  def app
    @app ||= Hastur::Service::RetrievalV2.new []
  end

  setup do
    @cass_client = mock("Cass client")
    ::Hastur::API::CassandraJavaClient.stubs(:new).with([]).returns(@cass_client)
    @cass_client.stubs(:status_check)
  end

  should "raise no error on /statusz" do
    get "/v2/statusz"
    assert true # Add to count
  end

  context "non-label query" do
    should "do simple lookup for fully-specified query" do
      #refute_nil sum
      #assert_kind_of Hash, sum
      #assert_equal UUID1_BAZ_SUM, sum[UUID1]["foo.bar.baz"].values.first
    end
  end
end
