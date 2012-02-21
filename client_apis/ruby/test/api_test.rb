$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")

require "test/unit"
require "hastur/api"
require "multi_json"

class HasturApiTest < Test::Unit::TestCase

  def setup
    Hastur::API.__test_mode__ = true
  end

  def teardown
    Hastur::API.__clear_msgs__
  end

  def test_counter
    curr_time = Time.now.to_i
    Hastur::API.counter("name", 1, curr_time)
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_equal("stat", hash[:_route].to_s)
    assert_equal("name", hash[:name])
    assert_equal(curr_time*1000000, hash[:timestamp])
    assert_equal(1, hash[:increment])
    assert_equal("counter", hash[:type].to_s)
    assert hash[:labels].keys.sort == [:app, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end

  def test_gauge
    curr_time = Time.now.to_i
    Hastur::API.gauge("name", 9, curr_time)
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_equal("stat", hash[:_route].to_s)
    assert_equal("name", hash[:name])
    assert_equal(curr_time * 1000000, hash[:timestamp])
    assert_equal(9, hash[:value])
    assert_equal("gauge", hash[:type].to_s)
    assert hash[:labels].keys.sort == [:app, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end
 
  def test_mark
    curr_time = Time.now.to_i
    Hastur::API.mark("myName", curr_time)
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_equal("stat", hash[:_route].to_s)
    assert_equal("myName", hash[:name])
    assert_equal("mark", hash[:type].to_s)
    assert_equal(curr_time*1000000, hash[:timestamp])
    assert hash[:labels].keys.sort == [:app, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end

  def test_heartbeat
    Hastur::API.heartbeat(nil, :app => "myApp")
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_equal("myApp", hash[:labels][:app])
    assert_equal("heartbeat", hash[:_route].to_s)
    assert hash[:labels].keys.sort == [:app, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end

  def test_client_heartbeat
    sleep 61    # wait for this heartbeat to come
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_not_nil hash
    assert_equal("heartbeat", hash[:_route].to_s)
    assert_equal("client_heartbeat", hash[:labels][:app])
    assert hash[:labels].keys.sort == [:app, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end

  def test_notification
    notification_msg = "This is my message"
    Hastur::API.notification(notification_msg, {:foo => "foo", :bar => "bar"})
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_equal("notification", hash[:_route].to_s)
    assert_equal(notification_msg, hash[:message])
    assert hash[:labels].keys.sort == [:app, :bar, :foo, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end

  def test_register_plugin
    plugin_path = "plugin_path"
    plugin_args = "plugin_args"
    plugin_name = "plugin_name"
    interval = 100
    labels = {:foo => "foo"}
    Hastur::API.register_plugin(plugin_path, plugin_args, plugin_name, interval, labels)
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1] 
    assert_equal("register_plugin", hash[:_route].to_s)
    assert_equal(plugin_path, hash[:plugin_path])
    assert_equal(plugin_args, hash[:plugin_args])
    assert_equal(plugin_name, hash[:plugin])
    assert_equal(interval, hash[:interval])
    assert hash[:labels].keys.sort == [:app, :foo, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end

  def test_register_service
    labels = {}
    Hastur::API.register_service(labels)
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_equal("register_service", hash[:_route].to_s)
    assert hash[:labels].keys.sort == [:app, :pid, :tid],
      "Wrong keys #{hash[:labels].keys.inspect} in default labels!"
  end
  
  def test_every
    s = "every_5_second_test"
    Hastur::API.every :five_secs do
      Hastur::API.mark(s, Time.now)
    end
    sleep 5
    msgs = Hastur::API.__test_msgs__
    hash = msgs[-1]
    assert_not_nil hash
    assert_equal(s, hash[:name])
  end
end
