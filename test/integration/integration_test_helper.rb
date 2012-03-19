#!/usr/bin/env ruby
# This must be first, before anything else
require_relative "../hastur_simplecov"

require "multi_json"
require "hastur-server/libc_ffi"
require 'hastur-server/sink/cassandra_schema'

# For testing Hastur components, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "lib")

require "nodule"
require "nodule/util"
require "hastur"

HASTUR_ROOT = File.join(File.dirname(__FILE__), "..", "..")

HASTUR_ROUTER_BIN="#{HASTUR_ROOT}/bin/hastur-router.rb"
HASTUR_CLIENT_BIN="#{HASTUR_ROOT}/bin/hastur-client.rb"
HASTUR_MSGTOOL_BIN="#{HASTUR_ROOT}/tools/msgtool.rb"
HASTUR_QUERY_SERVER_BIN="#{HASTUR_ROOT}/bin/hastur-query-server.rb"
HASTUR_CASS_SINK_BIN="#{HASTUR_ROOT}/bin/cass-sink.rb"
HASTUR_SCHEDULER="#{HASTUR_ROOT}/bin/run-scheduler.rb"

HASTUR_UDP_PORT = Nodule::Util.random_udp_port
Hastur.udp_port = HASTUR_UDP_PORT

# easy to spot fake UUID's
C1UUID = '11111111-2222-3333-4444-555555555555'
C2UUID = 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'
C3UUID = '66666666-7777-8888-9999-aaaaaaaaaaaa'

R1UUID = 'fafafafa-fafa-fafa-fafa-fafafafafafa'
R2UUID = '01010101-0101-0101-0101-010101010101'

MESSAGES = {
  :heartbeat_client => "{\"last_heartbeat\":\"2012-02-18 12:29:48 -0800\",\"heartbeat\":30}",
  :register_client  => "{\"uuid\":\"#{C1UUID}\",\"hostname\":\"web01.domain\",\"ipv4\":\"10.1.1.10\"}",
  :notification     => "{\"sla\":604800,\"app\":\"sinatra\",\"recipients\":[\"web-oncall\"]}",
  :stat             => "{\"name\":\"foo.bar.baz\",\"value\":1234,\"timestamp\":1234567890}",
  :log              => "{}",
  :error            => "{}",
}

def set_test_alarm(timeout=30)
  Signal.trap("ALRM") do
    assert false, "Timed out."
    Thread.list.each { |t| t.kill unless t == Thread.current }
    exit
  end
  LibC.alarm(timeout)
end

def cancel_test_alarm
  LibC.alarm(0)
end

def create_all_column_families(cassandra)
  cassandra.cli "--batch" do |process,stdin,stdout,stderr|
    # create the C* schema
    File.open(File.join(HASTUR_ROOT, 'tools', 'cassandra', 'create_keyspace.cass')).each do |line|
      unless line =~ /#/ or line.chomp.length == 0
        stdin.puts line
      end
    end
  end
end

def wait_for_cassandra_rows(client, cf, count, max_iterations=30)
  max_iterations.times do
    sleep 1
    client.each_key(cf) do |key|
      return true if client.count_columns(cf, key) >= count
    end
  end
  yield if block_given?
  false
end

