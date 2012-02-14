$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")

require "test/unit"
require "hastur/api"
require "multi_json"

class HasturApiTest < Test::Unit::TestCase

  def setup
    @server = UDPSocket.new
    @server.bind("127.0.0.1", 8125)
  end

  def teardown
    @server.close unless @server.closed?
  end

  def test_counter
    curr_time = Time.now.to_i
    Hastur::API.counter("name", 1, curr_time)
    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    assert_equal("name", hash['name'])
    assert_equal(curr_time, hash['timestamp'])
    assert_equal(1, hash['increment'])
    assert_equal("counter", hash['type'])
    assert_equal({}, hash['labels'])
  end

  def test_gauge
    curr_time = Time.now.to_i
    Hastur::API.gauge("name", 9, curr_time)
    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    assert_equal("name", hash['name'])
    assert_equal(curr_time, hash['timestamp'])
    assert_equal(9, hash['value'])
    assert_equal("gauge", hash['type'])
    assert_equal({}, hash['labels'])
  end
 
  def test_mark
    curr_time = Time.now.to_i
    Hastur::API.mark("name", curr_time)
    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    assert_equal("name", hash['name'])
    assert_equal("mark", hash['type'])
    assert_equal(curr_time, hash['timestamp'])
    assert_equal({}, hash['labels'])
  end

  def test_heartbeat
    Hastur::API.heartbeat("myApp", 2)
    sleep 2.5
    num_msgs = 0
    1.upto(2) do
      msg = @server.recvfrom(65000)[0]
      hash = MultiJson.decode msg
      assert_equal("myApp", hash['app'])
      assert_equal(2, hash['interval'])
      assert_equal("heartbeat", hash['_route'])
      num_msgs += 1
    end
    assert_equal(2, num_msgs);
  end

end
