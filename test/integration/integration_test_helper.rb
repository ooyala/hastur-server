#!/usr/bin/env ruby
# This must be first, before anything else
require_relative "../hastur_simplecov"

require "minitest/unit"
require "multi_json"
require "hastur-server/libc_ffi"
require "hastur-server/util"

# For testing Hastur components, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "lib")

require "nodule"
require "nodule/util"
require "hastur/api"

HASTUR_ROOT = File.join(File.dirname(__FILE__), "..", "..")

HASTUR_CORE_BIN="#{HASTUR_ROOT}/bin/hastur-core.rb"
HASTUR_AGENT_BIN="#{HASTUR_ROOT}/bin/hastur-agent.rb"
HASTUR_MSGTOOL_BIN="#{HASTUR_ROOT}/tools/msgtool.rb"
HASTUR_CASS_SINK_BIN="#{HASTUR_ROOT}/bin/cass-sink.rb"
HASTUR_REGISTRATION_ROLLUP_BIN="#{HASTUR_ROOT}/bin/registration-rollups.rb"

HASTUR_UDP_PORT = Nodule::Util.random_udp_port
Hastur.udp_port = HASTUR_UDP_PORT

# easy to spot fake UUID's
A1UUID = '11111111-2222-3333-4444-555555555555'
A2UUID = 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'
A3UUID = '66666666-7777-8888-9999-aaaaaaaaaaaa'

R1UUID = 'fafafafa-fafa-fafa-fafa-fafafafafafa'
R2UUID = '01010101-0101-0101-0101-010101010101'

TIMESTAMP = Hastur::Util.timestamp

EVENT = {
  :type      => :event,
  :name      => 'live.universe.everything',
  :subject   => "42",
  :body      => "The secret to Life, The Universe, and Everything! has been discovered. The universe will now be replaced.",
  :attn      => [ 'root@localhost', '5555555555@message.text.com' ],
  :timestamp => TIMESTAMP,
  :labels    => { :fake => true, :maybe => "what is 6 times 7?" }
}

EVENT_JSON = MultiJson.dump EVENT

def test_timeout(secs)
  ENV["IS_JENKINS"] ? secs + 60 : secs
end

def assert_json_not_empty(*strs)
  strs.flatten.each do |str|
    refute_nil str, "Json returned is nil"
    str2 = str.gsub(/\s+/, "")
    assert !str2.empty?, "Json returned is an empty string"
    assert str2 != "{}", "Json returned is empty json {}"
  end
end

def create_all_column_families(cassandra)
  cassandra.cli "--batch" do |process,stdin,stdout,stderr|
    stdin.sync = true
    # create the C* schema
    File.open(File.join(HASTUR_ROOT, 'tools', 'cassandra', 'create_keyspace.cass')).each do |line|
      unless line =~ /#/ or line.chomp.length == 0
        stdin.puts line
      end
    end
  end
  # TODO: Hack - figure out how to do this better
  # cassandra migrations are /really/ slow in jenkins
  sleep 60 if ENV["IS_JENKINS"]
end

def wait_for_cassandra_rows(client, cf, count, max_iterations=30, flunk_on_timeout=false)
  max_iterations.times do
    sleep 1
    client.each_key(cf) do |key|
      return true if client.count_columns(cf, key) >= count
    end
  end
  yield if block_given?
  if flunk_on_timeout
    flunk "timeout waiting for #{count} rows in cassandra column family '#{cf}'"
  end
  false
end

# counts total entries in all cols in a CF across all rows, returns the count
def cassandra_cf_value_count(client, cf)
  count = 0
  client.each_key(cf) do |key|
    count += client.count_columns(cf, key)
  end
  count
end

def hastur_proxy(port=HASTUR_UDP_PORT, method, message)
  Hastur.udp_port = port
  Hastur.send method, message
end
