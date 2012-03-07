require_relative "../../test_helper"

require "hastur-server/sink/cassandra_schema"

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

EVENT_JSON = <<JSON
{
  "severity": "bogus",
  "timestamp": 1329858724285438,
  "tags": [
    "backlot-oncall",
    "noah@ooyala.com",
    "big Jimmy"
  ],
  "labels": {
  }
}
JSON

FAKE_UUID = "fafafafa-fafa-fafa-fafa-fafafafafafa"
FAKE_UUID2 = "fafafafa-fafa-fafa-fafa-fafafafafaf2"
FAKE_UUID3 = "fafafafa-fafa-fafa-fafa-fafafafafaf3"
NOWISH_TIMESTAMP = 1330000400.to_s
class CassandraSchemaTest < Scope::TestCase
  setup do
    @cass_client = mock("Cassandra client")
    @cass_client.stubs(:batch).yields(@cass_client)
    Hastur::Util.stubs(:timestamp).with(nil).returns(NOWISH_TIMESTAMP)
    @cass_client.stubs(:insert).with(anything, anything, { "last_access" => NOWISH_TIMESTAMP })
    @cass_client.stubs(:insert).with(:UUIDDay, anything, { FAKE_UUID => "" })
  end

  context "Stats schema" do

    should "insert a gauge into StatsArchive and StatsGauge" do
      json = GAUGE_JSON
      row_key = "#{FAKE_UUID}-1329858600000000"
      day_ts = "1329782400000000"
      colname = "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      @cass_client.expects(:insert).with(:StatsGauge, row_key, { colname => (37.1).to_msgpack,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      @cass_client.expects(:insert).with(:StatNamesDay, day_ts, { "this.is.a.gauge" => "" })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert a counter into StatsArchive and StatsCounter" do
      json = COUNTER_JSON
      row_key = "#{FAKE_UUID}-1329858600000000"
      day_ts = "1329782400000000"
      colname = "totally.a.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      @cass_client.expects(:insert).with(:StatsCounter, row_key, { colname => 5.to_msgpack,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      @cass_client.expects(:insert).with(:StatNamesDay, day_ts, { "totally.a.counter" => "" })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert a mark into StatsArchive and StatsMark" do
      json = MARK_JSON
      row_key = "#{FAKE_UUID}-1329858600000000"
      day_ts = "1329782400000000"
      colname = "marky.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:StatsArchive, row_key, { colname => json,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      @cass_client.expects(:insert).with(:StatsMark, row_key, { colname => nil.to_msgpack,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      @cass_client.expects(:insert).with(:StatNamesDay, day_ts, { "marky.mark" => "" })
      Hastur::Cassandra.insert_stat(@cass_client, json, :uuid => FAKE_UUID)
    end

    should "insert an event into EventsArchive" do
      json = EVENT_JSON
      row_key = "#{FAKE_UUID}-1329782400000000"  # Time rounded down to day
      colname = "\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"  # Just time, no name
      @cass_client.expects(:insert).with(:EventsArchive, row_key, { colname => json,
                                           "last_access" => NOWISH_TIMESTAMP,
                                           "last_write" => NOWISH_TIMESTAMP }, {})
      Hastur::Cassandra.insert(@cass_client, json, "event", :uuid => FAKE_UUID)
    end

    should "query a stat from StatsGauge" do
      @cass_client.expects(:multi_get).with(:StatsGauge, [ "#{FAKE_UUID}-1329858600000000" ],
                                            :count => 10_000,
                                            :start => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "this.is.a.gauge", :gauge,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query a stat from StatsCounter" do
      @cass_client.expects(:multi_get).with(:StatsCounter, [ "#{FAKE_UUID}-1329858600000000" ],
                                            :count => 10_000,
                                            :start => "some.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "some.counter-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "some.counter", :counter,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query a stat from StatsMark" do
      @cass_client.expects(:multi_get).with(:StatsMark, [ "#{FAKE_UUID}-1329858600000000" ],
                                            :count => 10_000,
                                            :start => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "this.is.a.mark", :mark,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query a stat from StatsArchive" do
      @cass_client.expects(:multi_get).with(:StatsArchive, [ "#{FAKE_UUID}-1329858600000000" ],
                                            :count => 10_000,
                                            :start => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get_stat(@cass_client, FAKE_UUID, "this.is.a.mark", nil,
                                       1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query an event from EventsArchive" do
      @cass_client.expects(:multi_get).with(:EventsArchive, [ "#{FAKE_UUID}-1329782400000000" ],
                                            :count => 10_000,
                                            :start => "\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "event", 1329858724285438, 1329858724285440)
      assert_equal({}, out)
    end

    should "query an event from EventsArchive with multiple client UUIDs" do
      @cass_client.expects(:multi_get).with(:EventsArchive,
                                            [ "#{FAKE_UUID}-1329782400000000",
                                              "#{FAKE_UUID2}-1329782400000000",
                                              "#{FAKE_UUID3}-1329782400000000" ],
                                            :count => 10_000,
                                            :start => "\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :finish => "\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      out = Hastur::Cassandra.get(@cass_client, [ FAKE_UUID, FAKE_UUID2, FAKE_UUID3 ], "event",
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
        @cass_client.expects(:multi_get).with(:StatsMark, [ "#{FAKE_UUID}-1329858600000000" ],
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

        assert_equal({
                       "this.is.a.mark" => {
                         1329858724285438 => "",
                         1329858724285439 => "",
                         1329858724285440 => "",
                       }
                     }, out)
      end

      should "prepare stats from Cassandra representation with get_all_stats" do
        @cass_client.expects(:multi_get).with(:StatsMark, [ "#{FAKE_UUID}-1329858600000000" ], :count => 10_000).
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
