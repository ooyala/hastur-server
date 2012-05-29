require_relative "../test_helper"
require "minitest/autorun"
require "rack/test"
require "mocha"
require "hastur-server/service/retrieval"

FAKE_TS1 = 1338318149512052
FAKE_TS2 = 1338318149823411

AGENT_REG_1 = <<JSON
{
  "uuid": "#{A1UUID}",
  "type": "reg_agent",
  "timestamp": "#{FAKE_TS1}",
  "hostname": "fake1.sv2",
  "ipv4": "127.0.1.1",
  "labels": {
  }
}
JSON

AGENT_REG_2 = <<JSON
{
  "uuid": "#{A2UUID}",
  "type": "reg_agent",
  "timestamp": "#{FAKE_TS2}",
  "hostname": "fake2.sv2",
  "ipv4": "127.0.1.2",
  "labels": {
  }
}
JSON

class RetrievalServiceTest < MiniTest::Unit::TestCase
  include Rack::Test::Methods

  def app
    # Supply no URIs for Cassandra
    app = Hastur::Service::Retrieval.new []

    cass_client = mock("Cassandra Client")
    Hastur::Service::Retrieval.cass_client = cass_client

    # Supply fake agent registrations
    packed1 = [FAKE_TS1].pack("Q>")
    packed2 = [FAKE_TS2].pack("Q>")
    cass_client.stubs(:each).with(:RegAgentArchive).
      multiple_yields([A1UUID, { packed1 => AGENT_REG_1 }],
                      [A2UUID, { packed2 => AGENT_REG_2 }])

    app
  end

  def get_response_hash(uri, options = {})
    get uri
    #follow_redirect!

    #unless last_response.ok?
    #  raise "Request to #{uri} failed with status #{last_response.status}.  Body was: #{last_response.body}"
    #end

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

  def test_node
    hash = get_response_hash "/api/node"

    assert hash.keys.include?(A1UUID), "/api/node must include first UUID"
    assert hash.keys.include?(A2UUID), "/api/node must include second UUID, hash: #{hash.inspect}"
  end

end
