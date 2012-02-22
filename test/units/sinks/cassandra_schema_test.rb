require_relative "../../test_helper"

require "hastur/sink/cassandra_schema"

GAUGE_JSON = <<JSON
{
  "type": "gauge",
  "name": "this.is.a.gauge",
  "value": 37.1,
  "timestamp": 1329858724285438,
  "labels": {
    "a": 1,
    "b": 2
  }
}
JSON

COUNTER_JSON = <<JSON
{
  "type": "counter",
  "name": "totally.a.counter",
  "increment": 5,
  "timestamp": 1329858724285438,
  "labels": {
    "c": 1,
    "d": 2,
    "e": 9
  }
}
JSON

MARK_JSON = <<JSON
{
  "type": "mark",
  "name": "marky.mark",
  "timestamp": 1329858724285438,
  "labels": {
  }
}
JSON

FAKE_UUID = "fafafafa-fafa-fafa-fafa-fafafafafafa"

class CassandraSchemaTest < Scope::TestCase
  setup do
    @cass_client = mock("Cassandra client")
  end

  context "Stats schema" do

    should "insert a gauge into StatsArchive and StatsGauge" do
      json = GAUGE_JSON
      row_key = "#{FAKE_UUID}-1329858600"
      colname = "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json }, { :consistency => 2 })
      @cass_client.expects(:insert).with(:StatsGauge, row_key, { colname => "37.1" }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert a counter into StatsArchive and StatsCounter" do
      json = COUNTER_JSON
      row_key = "#{FAKE_UUID}-1329858600"
      colname = "totally.a.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json }, { :consistency => 2 })
      @cass_client.expects(:insert).with(:StatsCounter, row_key, { colname => "5" }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert a mark into StatsArchive and StatsMark" do
      json = MARK_JSON
      row_key = "#{FAKE_UUID}-1329858600"
      colname = "marky.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json }, { :consistency => 2 })
      @cass_client.expects(:insert).with(:StatsMark, row_key, { colname => "" }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "query a stat from StatsArchive" do
    end

  end
end
