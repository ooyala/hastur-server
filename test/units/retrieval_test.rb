require_relative "../test_helper"
require "rack/test"

require "multi_json"
MultiJson.use :json_gem

require "hastur-server/api/v2"
require "hastur-server/api/cass_java_client"

# Timestamp in seconds
NOWISH_TIMESTAMP = 1330000400000000

# Timestamps in microseconds, rounded down to various sizes.
ROW_5MIN_TS = 1329858600000000
ROW_HOUR_TS = 1329858000000000
ONE_DAY = 24 * 60 * 60 * 1_000_000
ROW_DAY_TS = Hastur::Cassandra.send(:time_segment_for_timestamp, ROW_5MIN_TS, ONE_DAY).to_s

class RetrievalServerTest < Scope::TestCase
  include Rack::Test::Methods

  def app
    @app ||= Hastur::Service::RetrievalV2.new []
  end

  setup do
    @cass_client = mock("Cass client")
    ::Hastur::API::CassandraJavaClient.stubs(:new).with([]).returns(@cass_client)
    @cass_client.stubs(:status_check)

    Hastur.stubs(:timestamp).returns(NOWISH_TIMESTAMP)
  end

  should "raise no error on /statusz" do
    get "/v2/statusz"
    assert true # Add to count
  end

  context "non-label query" do
    should "do simple lookup for fully-specified query" do
      out_hash = {
        A1UUID => {
          "bobs.gauge" => {
            NOWISH_TIMESTAMP => 37
          }
        }
      }

      Hastur::Cassandra.expects(:get).with(@cass_client, [A1UUID], ["gauge"],
                                           NOWISH_TIMESTAMP - 1, NOWISH_TIMESTAMP, {
        :name => "bobs.gauge",
        :value_only => true,
        :request_ts => NOWISH_TIMESTAMP}).returns(out_hash)

      result = get "/v2/query?type=gauge&ago=1&uuid=#{A1UUID}&name=bobs.gauge&kind=value"
      hash = MultiJson.load(result.body)
      assert_equal 37, hash[A1UUID]["bobs.gauge"][NOWISH_TIMESTAMP.to_s]
    end
  end
end
