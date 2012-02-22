require_relative "../../test_helper"

require "hastur/sink/cassandra_schema"

class CassandraSchemaTest < Scope::TestCase
  setup do
    @cass_client = mock("Cassandra client")
  end

  context "Stats schema" do

    should "insert a stat into StatsArchive" do
      uuid = "fafafafa-fafa-fafa-fafa-fafafafafafa"
      json = <<JSON
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
      row_key = "#{uuid}-1329858600"
      colname = "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json }, { :consistency => 2 })
      @cass_client.expects(:insert).with(:StatsGauge, row_key, { colname => "37.1" }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => uuid)
    end

  end
end
