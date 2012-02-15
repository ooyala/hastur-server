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
    assert_equal(curr_time*1000000, hash['timestamp'])
    assert_equal(1, hash['increment'])
    assert_equal("counter", hash['type'])
    assert hash['labels'].keys.sort == ["app", "pid", "tid"],
      "Wrong keys #{hash['labels'].keys.inspect} in default labels!"
  end

  def test_gauge
    curr_time = Time.now.to_i
    Hastur::API.gauge("name", 9, curr_time)
    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    assert_equal("name", hash['name'])
    assert_equal(curr_time * 1000000, hash['timestamp'])
    assert_equal(9, hash['value'])
    assert_equal("gauge", hash['type'])
    assert hash['labels'].keys.sort == ["app", "pid", "tid"],
      "Wrong keys #{hash['labels'].keys.inspect} in default labels!"
  end
 
  def test_mark
    curr_time = Time.now.to_i
    Hastur::API.mark("name", curr_time)
    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    assert_equal("name", hash['name'])
    assert_equal("mark", hash['type'])
    assert_equal(curr_time*1000000, hash['timestamp'])
    assert hash['labels'].keys.sort == ["app", "pid", "tid"],
      "Wrong keys #{hash['labels'].keys.inspect} in default labels!"
  end

  def test_heartbeat
    Hastur::API.heartbeat("myApp", nil, "app" => "myApp")

    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    STDERR.puts "Received hash: #{hash.inspect}"
    assert_equal("myApp", hash['labels']['app'])
    assert_equal("heartbeat", hash['_route'])
    assert hash['labels'].keys.sort == ["app", "pid", "tid"],
      "Wrong keys #{hash['labels'].keys.inspect} in default labels!"
  end

end
