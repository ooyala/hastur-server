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
      @cass_client.expects(:insert).with(:StatsGauge, row_key, { colname => (37.1).to_msgpack }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert a counter into StatsArchive and StatsCounter" do
      json = COUNTER_JSON
      row_key = "#{FAKE_UUID}-1329858600"
      colname = "totally.a.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json }, { :consistency => 2 })
      @cass_client.expects(:insert).with(:StatsCounter, row_key, { colname => 5.to_msgpack }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert a mark into StatsArchive and StatsMark" do
      json = MARK_JSON
      row_key = "#{FAKE_UUID}-1329858600"
      colname = "marky.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json }, { :consistency => 2 })
      @cass_client.expects(:insert).with(:StatsMark, row_key, { colname => "".to_msgpack }, { :consistency => 2 })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "query a stat from StatsGauge" do
      @cass_client.expects(:multi_get).with(:StatsGauge, [ "#{FAKE_UUID}-1329858600" ],
                                            :count => 10_000,
                                            :start => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "this.is.a.gauge", :gauge,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query a stat from StatsCounter" do
      @cass_client.expects(:multi_get).with(:StatsCounter, [ "#{FAKE_UUID}-1329858600" ],
                                            :count => 10_000,
                                            :start => "some.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "some.counter-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "some.counter", :counter,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query a stat from StatsMark" do
      @cass_client.expects(:multi_get).with(:StatsMark, [ "#{FAKE_UUID}-1329858600" ],
                                            :count => 10_000,
                                            :start => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "this.is.a.mark", :mark,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    context "Filtering stats from Cassandra representation" do
      setup do
        @coded_ts_37 = [1329858724285437].pack("Q>")
        @coded_ts_38 = [1329858724285438].pack("Q>")
        @coded_ts_39 = [1329858724285439].pack("Q>")
        @coded_ts_40 = [1329858724285440].pack("Q>")
        @coded_ts_41 = [1329858724285441].pack("Q>")
      end

      should "prepare a stat from Cassandra representation with get_stat" do
        @cass_client.expects(:multi_get).with(:StatsMark, [ "#{FAKE_UUID}-1329858600" ],
                                              :count => 10_000,
                                              :start => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                              :finish => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
          returns({
                    "uuid1-1234567890" => { },   # Delete row with empty hash
                    "uuid2-0987654321" => nil,   # Delete row with nil
                    "uuid3-1234500000" => {
                      "this.is.a.mark-#{@coded_ts_37}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_38}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_39}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_40}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_41}" => "".to_msgpack,
                    }
                  })
        out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "this.is.a.mark", :mark,
                                         1329858724285438, 1329858724285440)

        # get_stat relies on Cassandra to filter, so it returns all rows
        assert_equal({
                       "this.is.a.mark" => {
                         1329858724285437 => "",
                         1329858724285438 => "",
                         1329858724285439 => "",
                         1329858724285440 => "",
                         1329858724285441 => "",
                       }
                     }, out)
      end

      should "prepare stats from Cassandra representation with get_all_stats" do
        @cass_client.expects(:multi_get).with(:StatsMark, [ "#{FAKE_UUID}-1329858600" ], :count => 10_000).
          returns({
                    "uuid1-1234567890" => { },   # Delete row with empty hash
                    "uuid2-0987654321" => nil,   # Delete row with nil
                    "uuid3-1234500000" => {
                      "this.is.a.mark-#{@coded_ts_37}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_38}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_39}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_40}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_41}" => "".to_msgpack,
                      "this.mark-#{@coded_ts_37}" => "".to_msgpack,
                      "this.mark-#{@coded_ts_38}" => "".to_msgpack,
                      "this.mark-#{@coded_ts_40}" => "".to_msgpack,
                      "this.mark-#{@coded_ts_41}" => "".to_msgpack,
                    }
                  })
        out = Hastur::Cassandra.get_all_stats(@cass_client, FAKE_UUID,
                                              1329858724285438, 1329858724285440, :type => :mark)

        # get_all_stats filters rows by date
        assert_equal({
                       "this.is.a.mark" => {
                         1329858724285438 => "",
                         1329858724285439 => "",
                         1329858724285440 => "",
                       },
                       "this.mark" => {
                         1329858724285438 => "",
                         1329858724285440 => "",
                       }
                     }, out)
      end
    end

  end
end
