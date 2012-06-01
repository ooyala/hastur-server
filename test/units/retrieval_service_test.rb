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

INFO_OHAI_1 = <<JSON
{
  "uuid": "#{A1UUID}",
  "type": "info_ohai",
  "timestamp": "#{FAKE_TS1}",
  "labels": {
  }
}
JSON

INFO_OHAI_2 = <<JSON
{
  "uuid": "#{A2UUID}",
  "type": "info_ohai",
  "timestamp": "#{FAKE_TS2}",
  "labels": {
  }
}
JSON

EVENT_1 = <<JSON
{
  "uuid"      : "#{A1UUID}",
  "type"      : "event",
  "name"      : "live.universe.everything",
  "subject"   : "42",
  "body"      : "The secret to Life, The Universe, and Everything! has been discovered. The universe will now be replaced.",
  "attn"      : [ "root@localhost", "5555555555@message.text.com" ],
  "timestamp" : #{FAKE_TS1},
  "labels"    : { "fake": "true", "maybe": "what is 6 times 7?" }
}
JSON

EVENT_2 = <<JSON
{
  "uuid"      : "#{A1UUID}",
  "type"      : "event",
  "name"      : "dead.universe.everything",
  "subject"   : "42.5",
  "body"      : "The secret to Death, The Universe, and Everything! has been discovered. The universe will now be replaced.",
  "attn"      : [ "root@localhost", "5555555555@message.text.com" ],
  "timestamp" : #{FAKE_TS2},
  "labels"    : { "fake": "true", "maybe": "what is 6 times 1347?" }
}
JSON

class RetrievalServiceTest < MiniTest::Unit::TestCase
  include Rack::Test::Methods

  def app
    # Supply no URIs for Cassandra
    app = Hastur::Service::Retrieval.new []

    # Fake a Cassandra client and monkey patch it in.  We mock to avoid
    # using it, though.
    def app.cass_client
      @cass_client = mock("Cassandra Client")
    end

    app
  end

  def get_response_data(uri, options = {})
    get uri
    #follow_redirect!

    if last_response.ok?
      MultiJson.load(last_response.body)
    else
      File.open("/tmp/retrieval_test_body.html", "w") { |f| f.write(last_response.body) }
      assert last_response.ok?, "Request to #{uri} must succeed, not fail with #{last_response.status}." +
        "  Body is in /tmp/retrieval_test_body.html"
    end
  end

  def test_top_level
    hash = get_response_data "/api"

    assert hash.keys.include?("node"), "Must contain Node URL"
  end

  def test_types
    hash = get_response_data "/api/type"

    assert hash.keys.include?("all"), "Types hash must include 'all'"
    assert hash.keys.include?("stat"), "Types hash must include 'stat'"
    assert hash.keys.include?("event"), "Types hash must include 'event'"
  end

  def test_node
    Hastur::Cassandra.expects(:lookup_by_key).with(anything, :uuid, FAKE_TS1, FAKE_TS2).
      returns({ A1UUID => "", A2UUID => "" })

    hash = get_response_data "/api/node?start=#{FAKE_TS1}&end=#{FAKE_TS2}"

    assert hash.keys.include?(A1UUID), "/api/node must include first UUID"
    assert hash.keys.include?(A2UUID), "/api/node must include second UUID, hash: #{hash.inspect}"
  end

  def test_node_uuid
    Hastur::Cassandra.expects(:lookup_by_key).with(anything, :uuid, FAKE_TS1, FAKE_TS2).
      returns({ A1UUID => "", A2UUID => "" })

    array = get_response_data "/api/node/#{A1UUID}?start=#{FAKE_TS1}&end=#{FAKE_TS2}"

    assert_equal 1, array.size, "Must return an array of one response, not #{array.inspect}"
    assert array[0].keys.sort == [ "message_data", "ohai", "registration_data" ],
      "Returned response must link to appropriate services!"
  end

  INFO_OHAI = {
    A1UUID => {
      "info_ohai" => {
        nil => {
          FAKE_TS1 => INFO_OHAI_1,
        }
      }
    },
    A2UUID => {
      "info_ohai" => {
        nil => {
          FAKE_TS2 => INFO_OHAI_2,
        }
      }
    },
  }

  def test_node_uuid_ohai
    Hastur::Cassandra.expects(:get).with(anything, [A1UUID, A2UUID], "info_ohai",
                                         FAKE_TS1, FAKE_TS2, :count => 1).
      returns(INFO_OHAI)
    array = get_response_data "/api/node/#{A1UUID},#{A2UUID}/ohai?start=#{FAKE_TS1}&end=#{FAKE_TS2}"

    assert array.size == 2, "Must return two pieces of Ohai data"
    assert array.map { |ohai| ohai["uuid"] }.sort == [A1UUID, A2UUID], "Must have Ohai data for both UUIDs!"
  end

  def test_app
    Hastur::Cassandra.expects(:lookup_by_key).with(anything, :app_name, FAKE_TS1, FAKE_TS2).
      returns({ "app_name-37-#{A1UUID}" => "", "other_app-#{A2UUID}" => "" })

    hash = get_response_data "/api/app?start=#{FAKE_TS1}&end=#{FAKE_TS2}"
    assert hash.has_key?("app_name-37"), "Must have first app name!"
    assert hash.has_key?("other_app"), "Must have second app name!"
  end

  def test_app_app
    Hastur::Cassandra.expects(:lookup_by_key).with(anything, :app_name, FAKE_TS1, FAKE_TS2).
      returns({
                "app_name-37-#{A1UUID}" => "",
                "app_name-37-#{A2UUID}" => "",
                "other_app-#{A2UUID}" => "",
                "third$%app-#{A1UUID}" => "",
                "fourthapp-#{A3UUID}" => "",
              })

    array = get_response_data "/api/app/other_app,app_name-37,third%24%25app,not_an_app?" +
      "start=#{FAKE_TS1}&end=#{FAKE_TS2}"
    assert_equal 4, array.size, "Must return four app names"

    # Sort by app name
    sorted = array.sort { |a, b| a["app"] <=> b["app"] }

    # Sorted, the four names are: app_name-37, not_an_app, other_app, third$%app

    assert_equal [ A1UUID, A2UUID ], sorted[0]["nodes"], "Must have two UUIDs for app_name-37"
    assert_equal [], sorted[1]["nodes"], "Must have no UUIDs for not_an_app"
    assert_equal [ A2UUID ], sorted[2]["nodes"], "Must have one UUID for other_app"
    assert_equal [ A1UUID ], sorted[3]["nodes"], "Must have one UUID for third$%app"
  end

  TYPES = Hastur::Service::Retrieval::TYPES

  def test_data_node_uuid_format
    Hastur::Cassandra.expects(:get).with(anything, [A1UUID], TYPES[:all], FAKE_TS1, FAKE_TS2, {}).
      returns({
                A1UUID => {
                  "reg_agent" => {
                    "" => {
                      FAKE_TS1 => AGENT_REG_1,
                    }
                  }
                }
              })

    hash = get_response_data "/api/data/node/#{A1UUID}/message?start=#{FAKE_TS1}&end=#{FAKE_TS2}"

    assert_equal( { A1UUID => { "" => { FAKE_TS1.to_s => AGENT_REG_1 } },
                    "count" => 1, "uuid_count" => 1, "name_count" => 1 }, hash )
  end

  def test_retrieval_multiple_types
    Hastur::Cassandra.expects(:get).with(anything, [A1UUID],
                                         TYPES[:stat] + TYPES[:event] + TYPES[:heartbeat],
                                         FAKE_TS1, FAKE_TS2, {}).
      returns({
                A1UUID => {
                  "reg_agent" => {
                    "" => {
                      FAKE_TS1 => AGENT_REG_1,
                    }
                  }
                }
              })

    hash = get_response_data "/api/data/node/#{A1UUID}/type/stat,event,heartbeat" +
      "/message?start=#{FAKE_TS1}&end=#{FAKE_TS2}"
  end

  def test_retrieval_name_prefix
    Hastur::Cassandra.expects(:get).with(anything, [A1UUID], ["event"],
                                         FAKE_TS1, FAKE_TS2, { :name_prefix => "live" }).
      returns({
                A1UUID => {
                  "event" => {
                    "live.universe.everything" => {
                      FAKE_TS1 => EVENT_1,
                    }
                  }
                }
              })

    hash = get_response_data "/api/data/node/#{A1UUID}/type/event/name" +
      "/live*/message?start=#{FAKE_TS1}&end=#{FAKE_TS2}"
  end

  def test_retrieval_multiple_name_prefix
    Hastur::Cassandra.expects(:get).with(anything, [A1UUID, A2UUID], ["event"],
                                         FAKE_TS1, FAKE_TS2, { :name_prefix => "live" }).
      returns({
                A1UUID => {
                  "event" => {
                    "live.universe.everything" => {
                      FAKE_TS1 => EVENT_1,
                    }
                  }
                }
              })

    Hastur::Cassandra.expects(:get).with(anything, [A1UUID, A2UUID], ["event"],
                                         FAKE_TS1, FAKE_TS2, { :name_prefix => "dead" }).
      returns({
                A2UUID => {
                  "event" => {
                    "dead.universe.everything" => {
                      FAKE_TS2 => EVENT_2,
                    }
                  }
                }
              })

    Hastur::Cassandra.expects(:get).with(anything, [A1UUID, A2UUID], ["event"],
                                         FAKE_TS1, FAKE_TS2, { :name => "bobo" }).
      returns({ })

    hash = get_response_data "/api/data/node/#{A1UUID},#{A2UUID}/type/event/name" +
      "/live*,bobo,dead*/message?start=#{FAKE_TS1}&end=#{FAKE_TS2}"
  end

  # Next TODOs:
  #   * test output formats
  #   * test reversed, limit, consistency - Cassandra options
  #   * test count_columns, incl. in cass_schema_test

end
