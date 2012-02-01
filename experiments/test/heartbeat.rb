$LOAD_PATH.unshift "lib"

require "topology_helper"
require "assertions"

include Hastur::Test::Assert

def test(topology_file)
  load topology_file

  # TODO(viet): Refactor all of this stuff out into its own reusable module
  t = Hastur::Test::Topology.new(TOPOLOGY)
  t.start_all

  # start the actual testcase
  sleep 60
  # TODO(viet): Build a mechanism to retrieve messages from 
  #             each node in the topology and verify that heartbeat
  #             messages are being received.
  heartbeat_msgs = t.packets(WORKERS[0])

  # TODO(viet): assert that all of the heartbeats are showing up
  assert(2, heartbeat_msgs.size)
  packet_list_equal([ {:method => "heartbeat", :uuid => CLIENTS[0]},
                    {:method => "heartbeat", :uuid => CLIENTS[1]} ], heartbeat_msgs)

  t.stop CLIENTS[0]

  sleep 60

  # TODO(viet): assert that only 1/2 of the heartbeats are showing up
end

test( "data/topology1.rb" )
