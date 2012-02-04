$LOAD_PATH.unshift File.join(File.dirname(__FILE__), ".")

require "test/unit"
require "integration_test_helper"

class TopologyHelperTest < Test::Unit::TestCase
  def test_topology_setup
    h= [{
      :name => :client1, 
      :command => "#{HASTUR_ROOT}/bin/hastur-client.rb --router tcp://127.0.0.1:4321 --port 8125"
    }]
    t = Hastur::Test::Topology.new(h)
    t.start_all
    assert_equal(1, t.processes.size)
    assert_equal(1, t.process_names.size)
    assert_not_nil(t.processes[:client1][:pid])
    assert_equal(:client1, t.process_names[0])
    t.stop :client1
    assert_equal(1, t.processes.size)
    assert_equal(1, t.process_names.size)
    assert_nil(t.processes[:client1][:pid])
  end

  def test_stop_all
    h= [{
      :name => :client1, 
      :command => "#{HASTUR_ROOT}/bin/hastur-client.rb --router tcp://127.0.0.1:4321 --port 8125"
      }, {
      :name => :client2, 
      :command => "#{HASTUR_ROOT}/bin/hastur-client.rb --router tcp://127.0.0.1:4321 --port 8125"
      }]
    t = Hastur::Test::Topology.new(h)
    t.start_all
    assert_equal(2, t.processes.size)
    assert_equal(2, t.process_names.size)
    pid1 = t.processes[:client1][:pid]
    pid2 = t.processes[:client2][:pid]
    assert_not_nil(t.processes[:client1][:pid])
    assert_equal(:client1, t.process_names[0])
    assert_not_nil(t.processes[:client2][:pid])
    assert_equal(:client2, t.process_names[1])
    t.stop_all
    assert_equal(2, t.processes.size)
    assert_equal(2, t.process_names.size)
    assert_nil(t.processes[:client1][:pid])
    assert_nil(t.processes[:client2][:pid])
    assert_raise Errno::ESRCH do
      Process.kill(9, pid1)
    end
    assert_raise Errno::ESRCH do
      Process.kill(9, pid2)
    end
  end
end
