require_relative "lib/topology_helper"

def test(topology_file)
  load topology_file

  # TODO(viet): Refactor all of this stuff out into its own reusable module
  t = Hastur::Test::Topology.new
  t.build(TOPOLOGY)   # TOPOLOGY comes from topology_file
  t.start_all

  # start the actual testcase
  sleep 60
  # TODO(viet): Build a mechanism to retrieve messages from 
  #             each node in the topology and verify that heartbeat
  #             messages are being received.

end

test( "data/topology1.rb" )
