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
    @server.close if @server.closed?
  end

  def test_mark
    curr_time = Time.now.to_i
    Hastur::API.mark("name", curr_time)
    msg = @server.recvfrom(65000)[0]
    hash = MultiJson.decode msg
    assert_equal("name", hash['name'])
    assert_equal(curr_time, hash['timestamp'])
    assert_equal({}, hash['labels'])
  end

end
