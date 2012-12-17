require_relative "../../test_helper"

require "hastur-server/cassandra/schema"

GAUGE_JSON = <<JSON
{
  "type": "gauge",
  "uuid": "fafafafa-fafa-fafa-fafa-fafafafafafa",
  "name": "this.is.a.gauge",
  "value": 37.1,
  "timestamp": 1329858724285438,
  "labels": {
    "a": 1,
    "b": 2,
    "app": "myapp"
  }
}
JSON

COUNTER_JSON = <<JSON
{
  "type": "counter",
  "uuid": "fafafafa-fafa-fafa-fafa-fafafafafafa",
  "name": "totally.a.counter",
  "value": 5,
  "timestamp": 1329858724285438,
  "labels": {
    "c": 1,
    "d": 2,
    "e": 9,
    "app": "myapp"
  }
}
JSON

MARK_JSON = <<JSON
{
  "type": "mark",
  "uuid": "fafafafa-fafa-fafa-fafa-fafafafafafa",
  "name": "marky.mark",
  "value": "start",
  "timestamp": 1329858724285438,
  "labels": {
    "app": "myapp"
  }
}
JSON

EVENT_JSON = <<JSON
{
  "type": "event",
  "uuid": "fafafafa-fafa-fafa-fafa-fafafafafafa",
  "name": "fake.event.name",
  "timestamp": 1329858724285438,
  "body": "stack trace placeholder",
  "attn": [
    "backlot-oncall",
    "noah@ooyala.com",
    "big Jimmy"
  ],
  "labels": {
    "app": "myapp"
  }
}
JSON

REG_PROCESS_JSON = <<JSON
{
  "type": "reg_process",
  "uuid": "fafafafa-fafa-fafa-fafa-fafafafafafa",
  "data": { "a": "b", "c": "d" },
  "timestamp": 1329858724285438,
  "labels": {
    "app": "myapp"
  }
}
JSON

FAKE_UUID = "fafafafa-fafa-fafa-fafa-fafafafafafa"
FAKE_UUID2 = "fafafafa-fafa-fafa-fafa-fafafafafaf2"
FAKE_UUID3 = "fafafafa-fafa-fafa-fafa-fafafafafaf3"
NOWISH_TIMESTAMP = 1330000400.to_s

# Row timestamp rounded to nearest 5 minutes
ROW_TS = 1329858600000000

# Row timestamp rounded to nearest hour
ROW_HOUR_TS = 1329858000000000

ROW_DAY_TS = Hastur::Cassandra.send(:time_segment_for_timestamp, ROW_TS, Hastur::Cassandra::ONE_DAY).to_s

# for cassandra consistency
ONE = 1
DEFOPTS = { :consistency => ONE }

class CassandraSchemaTest < Scope::TestCase
  setup do
    @cass_client = mock("Cassandra client")
    @cass_client.stubs(:batch).yields(@cass_client)
    Hastur::Util.stubs(:timestamp).with(nil).returns(NOWISH_TIMESTAMP.to_i)
  end

  context "Cassandra message schema" do
    should "insert a gauge into gauge_archive and gauge_value" do
      json = GAUGE_JSON
      row_key = "#{FAKE_UUID}-#{ROW_TS}"
      colname = "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:gauge_archive, row_key, { colname => json }, DEFOPTS)
      @cass_client.expects(:insert).with(:gauge_value, row_key, anything, DEFOPTS)
      @cass_client.expects(:insert).with(:gauge_metadata, row_key,
                                         { "last_write" => NOWISH_TIMESTAMP,
                                           "last_access" => NOWISH_TIMESTAMP }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "uuid-#{ROW_DAY_TS}", { FAKE_UUID => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "name-#{ROW_DAY_TS}",
                                         { "this.is.a.gauge-11-#{FAKE_UUID}" => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "app_name-#{ROW_DAY_TS}", { "myapp-#{FAKE_UUID}" => ""}, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_label', "uuid-#{ROW_HOUR_TS}", anything, anything).times(3);
      @cass_client.expects(:insert).with('lookup_by_label', "statname-#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything).times(3);
      @cass_client.expects(:insert).with('gauge_label_index', "#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything).times(3);
      Hastur::Cassandra.insert(@cass_client, json, "gauge", :consistency => ONE)
    end

    should "insert a counter into counter_archive and counter_value" do
      json = COUNTER_JSON
      row_key = "#{FAKE_UUID}-#{ROW_TS}"
      colname = "totally.a.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:counter_archive, row_key, { colname => json }, DEFOPTS)
      @cass_client.expects(:insert).with(:counter_value, row_key, anything, DEFOPTS)
      @cass_client.expects(:insert).with(:counter_metadata, row_key,
                                         { "last_write" => NOWISH_TIMESTAMP,
                                           "last_access" => NOWISH_TIMESTAMP }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "uuid-#{ROW_DAY_TS}", { FAKE_UUID => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "name-#{ROW_DAY_TS}",
                                         { "totally.a.counter-12-#{FAKE_UUID}" => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "app_name-#{ROW_DAY_TS}", { "myapp-#{FAKE_UUID}" => ""}, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_label', "uuid-#{ROW_HOUR_TS}", anything, anything).times(4);
      @cass_client.expects(:insert).with('lookup_by_label', "statname-#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything).times(4);
      @cass_client.expects(:insert).with('counter_label_index', "#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything).times(4);
      Hastur::Cassandra.insert(@cass_client, json, "counter", :consistency => ONE)
    end

    should "insert a mark into mark_archive and mark_value" do
      json = MARK_JSON
      row_key = "#{FAKE_UUID}-#{ROW_HOUR_TS}"
      colname = "marky.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:mark_archive, row_key, { colname => json }, DEFOPTS)
      @cass_client.expects(:insert).with(:mark_value, row_key, anything, DEFOPTS)
      @cass_client.expects(:insert).with(:mark_metadata, row_key,
                                         { "last_write" => NOWISH_TIMESTAMP,
                                           "last_access" => NOWISH_TIMESTAMP }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "uuid-#{ROW_DAY_TS}", { FAKE_UUID => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "name-#{ROW_DAY_TS}",
                                         { "marky.mark-10-#{FAKE_UUID}" => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "app_name-#{ROW_DAY_TS}", { "myapp-#{FAKE_UUID}" => ""}, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_label', "uuid-#{ROW_HOUR_TS}", anything, anything);
      @cass_client.expects(:insert).with('lookup_by_label', "statname-#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything);
      @cass_client.expects(:insert).with('mark_label_index', "#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything);
      Hastur::Cassandra.insert(@cass_client, json, "mark", :consistency => ONE)
    end

    should "insert an event into event_archive" do
      json = EVENT_JSON
      row_key = "#{FAKE_UUID}-1329782400000000"  # Time rounded down to day
      colname = "fake.event.name-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:event_archive, row_key, { colname => json }, DEFOPTS)
      @cass_client.expects(:insert).with(:event_metadata, row_key,
                                         { "last_write" => NOWISH_TIMESTAMP,
                                           "last_access" => NOWISH_TIMESTAMP }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "uuid-#{ROW_DAY_TS}", { FAKE_UUID => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "name-#{ROW_DAY_TS}",
                                         { "fake.event.name-1-#{FAKE_UUID}" => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "app_name-#{ROW_DAY_TS}", { "myapp-#{FAKE_UUID}" => ""}, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_label', "uuid-#{ROW_HOUR_TS}", anything, anything);
      @cass_client.expects(:insert).with('lookup_by_label', "statname-#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything);
      @cass_client.expects(:insert).with('event_label_index', "#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything);
      Hastur::Cassandra.insert(@cass_client, json, "event", :consistency => ONE)
    end

    should "insert a reg_process into reg_process_archive" do
      json = REG_PROCESS_JSON
      row_key = "#{FAKE_UUID}-1329782400000000"  # Time rounded down to day
      colname = "\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE"
      @cass_client.expects(:insert).with(:reg_process_archive, row_key, { colname => json }, DEFOPTS)
      @cass_client.expects(:insert).with(:reg_process_metadata, row_key,
                                         { "last_write" => NOWISH_TIMESTAMP,
                                           "last_access" => NOWISH_TIMESTAMP }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "uuid-#{ROW_DAY_TS}", { FAKE_UUID => "" }, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_key', "app_name-#{ROW_DAY_TS}", { "myapp-#{FAKE_UUID}" => ""}, DEFOPTS)
      @cass_client.expects(:insert).with('lookup_by_label', "uuid-#{ROW_HOUR_TS}", anything, anything);
      @cass_client.expects(:insert).with('lookup_by_label', "statname-#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything);
      @cass_client.expects(:insert).with('reg_process_label_index', "#{FAKE_UUID}-#{ROW_HOUR_TS}", anything, anything);
      Hastur::Cassandra.insert(@cass_client, json, "reg_process", :consistency => ONE)
    end

    should "query a gauge from gauge_value" do
      @cass_client.expects(:multi_get).with(:gauge_value, [ "#{FAKE_UUID}-#{ROW_TS}" ],
                                            :consistency => ONE,
                                            :count => 10_000,
                                            :finish => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :start => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      @cass_client.expects(:insert).with(:gauge_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "gauge",
                                  1329858724285438, 1329858724285440,
                                  :name => "this.is.a.gauge", :value_only => true, :consistency => ONE)
      assert_equal({}, out)
    end

    should "query a counter from counter_value" do
      @cass_client.expects(:multi_get).with(:counter_value, [ "#{FAKE_UUID}-#{ROW_TS}" ],
                                            :consistency => ONE,
                                            :count => 10_000,
                                            :finish => "some.counter-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :start => "some.counter-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      @cass_client.expects(:insert).with(:counter_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "counter",
                                  1329858724285438, 1329858724285440,
                                  :name => "some.counter", :value_only => true, :consistency => ONE)
      assert_equal({}, out)
    end

    should "query a mark from mark_value" do
      @cass_client.expects(:multi_get).with(:mark_value, [ "#{FAKE_UUID}-#{ROW_HOUR_TS}" ],
                                            :consistency => ONE,
                                            :count => 10_000,
                                            :finish => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :start => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      @cass_client.expects(:insert).with(:mark_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "mark",
                                  1329858724285438, 1329858724285440,
                                  :name => "this.is.a.mark", :value_only => true, :consistency => ONE)
      assert_equal({}, out)
      assert_equal({}, out)
    end

    should "query a gauge from gauge_archive" do
      @cass_client.expects(:multi_get).with(:gauge_archive, [ "#{FAKE_UUID}-#{ROW_TS}" ],
                                            :consistency => ONE,
                                            :count => 10_000,
                                            :finish => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :start => "this.is.a.gauge-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      @cass_client.expects(:insert).with(:gauge_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "gauge",
                                       1329858724285438, 1329858724285440, :name => "this.is.a.gauge",
                                       :consistency => ONE)
      assert_equal({}, out)
    end

    should "query an event from event_archive" do
      @cass_client.expects(:multi_get).with(:event_archive, [ "#{FAKE_UUID}-1329782400000000" ],
                                            :consistency => ONE, :count => 10_000).
        returns({})
      @cass_client.expects(:insert).with(:event_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "event",
                                       1329858724285438, 1329858724285440, DEFOPTS)
      assert_equal({}, out)
    end

    should "query an info_process from info_process_archive" do
      @cass_client.expects(:multi_get).with(:info_process_archive, [ "#{FAKE_UUID}-1329782400000000" ],
                                            :consistency => ONE,
                                            :count => 10_000,
                                            :finish => "\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                            :start => "\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
        returns({})
      @cass_client.expects(:insert).with(:info_process_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "info_process",
                                       1329858724285438, 1329858724285440, DEFOPTS)
      assert_equal({}, out)
    end

    should "query an event from event_archive with multiple client UUIDs" do
      day_ts = "1329782400000000"
      @cass_client.expects(:multi_get).with(:event_archive,
                                            [ "#{FAKE_UUID}-#{day_ts}",
                                              "#{FAKE_UUID2}-#{day_ts}",
                                              "#{FAKE_UUID3}-#{day_ts}" ],
                                            :consistency => ONE, :count => 10_000).
        returns({})
      @cass_client.expects(:insert).with(:event_metadata, "#{FAKE_UUID}-#{day_ts}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      @cass_client.expects(:insert).with(:event_metadata, "#{FAKE_UUID2}-#{day_ts}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      @cass_client.expects(:insert).with(:event_metadata, "#{FAKE_UUID3}-#{day_ts}",
                                         { "last_access" => NOWISH_TIMESTAMP}, {})
      out = Hastur::Cassandra.get(@cass_client, [ FAKE_UUID, FAKE_UUID2, FAKE_UUID3 ], "event",
                                  1329858724285438, 1329858724285440, DEFOPTS)
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

      should "prepare a stat from Cassandra representation with get" do
        @cass_client.expects(:multi_get).with(:mark_value, [ "#{FAKE_UUID}-#{ROW_HOUR_TS}" ],
                                              :consistency => ONE,
                                              :count => 10_000,
                                              :finish => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCB\xFE",
                                              :start => "this.is.a.mark-\x00\x04\xB9\x7F\xDC\xDC\xCC\x00").
          returns({
                    "#{FAKE_UUID2}-1234567890" => { },   # Delete row with empty hash
                    "#{FAKE_UUID3}-0987654321" => nil,   # Delete row with nil
                    "#{FAKE_UUID}-1234500000" => {
                      "this.is.a.mark-#{@coded_ts_37}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_38}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_39}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_40}" => "".to_msgpack,
                      "this.is.a.mark-#{@coded_ts_41}" => "".to_msgpack,
                    }
                  })
        @cass_client.expects(:insert).with(:mark_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                           { "last_access" => NOWISH_TIMESTAMP}, {})
        out = Hastur::Cassandra.get(@cass_client, FAKE_UUID, "mark",
                                    1329858724285438, 1329858724285440,
                                    :name => "this.is.a.mark", :value_only => true, :consistency => ONE)

        assert_equal({
                       "#{FAKE_UUID}" => {
                         "mark" => {
                           "this.is.a.mark" => {
                             1329858724285438 => "",
                             1329858724285439 => "",
                             1329858724285440 => "",
                           }
                         }
                       }
                     }, out)
      end

      should "prepare stats from Cassandra representation with get_all_stats" do
        get_opts = DEFOPTS.merge(:count => 10_000)
        @cass_client.expects(:multi_get).with(:mark_value, [ "#{FAKE_UUID}-#{ROW_HOUR_TS}" ], get_opts).
          returns({
                    "#{FAKE_UUID2}-1234567890" => { },   # Delete row with empty hash
                    "#{FAKE_UUID3}-0987654321" => nil,   # Delete row with nil
                    "#{FAKE_UUID}-1234500000"  => {
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
        @cass_client.expects(:multi_get).with(:gauge_value, [ "#{FAKE_UUID}-#{ROW_TS}" ], get_opts).
          returns({})
        @cass_client.expects(:multi_get).with(:counter_value, [ "#{FAKE_UUID}-#{ROW_TS}" ], get_opts).
          returns({})
        @cass_client.expects(:insert).with(:mark_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                           { "last_access" => NOWISH_TIMESTAMP}, {})
        @cass_client.expects(:insert).with(:gauge_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                           { "last_access" => NOWISH_TIMESTAMP}, {})
        @cass_client.expects(:insert).with(:counter_metadata, "#{FAKE_UUID}-#{ROW_DAY_TS}",
                                           { "last_access" => NOWISH_TIMESTAMP}, {})
        out = Hastur::Cassandra.get_all_stats(@cass_client, FAKE_UUID,
                                              1329858724285438, 1329858724285440,
                                              :value_only => true, :consistency => ONE)

        # get_all_stats filters rows by date
        assert_equal({
                       "#{FAKE_UUID}" => {
                         "mark" => {
                           "this.is.a.mark" => {
                             1329858724285438 => "",
                             1329858724285439 => "",
                             1329858724285440 => "",
                           },
                           "this.mark" => {
                             1329858724285438 => "",
                             1329858724285440 => "",
                           }
                         }
                       }
                     }, out)
      end

    end
  end
end
