#
# THIS CODE DOES NOT RUN. VIEWER DISCRETION IS ADVISED.
#

def verify expected_hash, msgs
  assert(msgs.size == 3)
  # verifies the JSON format
  hash = MultiJson.decode msgs[-1] rescue raise "Expected valid JSON, but was not => #{msgs[-1]}"
  # verify the correctness of the data
  Hastur::Daemon::Verify.client_uuid( msgs[-3], expected_hash['client_uuid'] )
  Hastur::Daemon::Verify.routing_key( msgs[-2], expected_hash['routing_key'] )
  Hastur::Daemon::Verify.method( hash, expected_hash['method'] )
  "Success"
end

# TODO: Retrieve the inputs and expected values from test file
expected_hash = ...
input_hash    = ...

input_msg = MultiJson.decode input_hash['payload']

# TODO: Retrieve the topology of the test
t = Hastur::Topology.create input_hash['topology']

# Send messages and verify on the fly
input_hash['number_of_iterations'].times do |iteration|
  # TODO: improve this to send to the right client, not just anyone
  send input_msg
  # Let the sinks catch up with receiving messages
  sleep input_hash['timeout']
  #
  # Psuedo code to get the messages from a sink
  # Expectation is that the messages are in the following format
  #   [computer name]
  #   [client UUID]
  #   [routing key]
  #   [json payload]
  #
  msgs = ...
  puts "[#{iteration}] #{verify(expected_hash, msgs)}"
end





