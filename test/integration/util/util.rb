def send_2_heartbeat(port1, port2, msg1, msg2)
  Hastur.udp_port = port1
  Hastur.heartbeat(msg1)
  Hastur.udp_port = port2
  Hastur.heartbeat(msg2)
  sleep 3
end

def ensure_heartbeats(both, msg1, msg2, num_msgs_1, num_msgs_2, sinatra_port)
  # Query from 10 minutes ago to 10 minutes from now, just to grab everything
  start_ts = Hastur.timestamp(Time.now.to_i - 600)
  end_ts = Hastur.timestamp(Time.now.to_i + 600)

  url1 = "http://127.0.0.1:#{sinatra_port}/data/heartbeat/json?uuid=#{A1UUID}&start=#{start_ts}&end=#{end_ts}"
  url2 = "http://127.0.0.1:#{sinatra_port}/data/heartbeat/json?uuid=#{A2UUID}&start=#{start_ts}&end=#{end_ts}"
  a1_messages = open(url1).read
  a2_messages = open(url2).read

  # ensure that there is at least something in the C*
  assert_json_not_empty a1_messages
  assert_json_not_empty a2_messages
 
  # attempt to parse the data
  a1_hashes = MultiJson.decode(a1_messages)
  a2_hashes = MultiJson.decode(a2_messages)

  # check for accurate data
  assert_not_nil(a1_hashes[msg1])
  assert_equal(num_msgs_1, a1_hashes[msg1].keys.size)
  a1_hashes[msg1].keys.each do |timestamp|
    assert_equal(msg1, a1_hashes[msg1][timestamp]["name"])
  end
  # check for the second client data if needed
  assert_not_nil(a2_hashes[msg2])
  assert_equal(num_msgs_2, a2_hashes[msg2].keys.size)
  a2_hashes[msg2].keys.each do |timestamp|
    assert_equal(msg2, a2_hashes[msg2][timestamp]["name"])
  end
end
