require "rubygems"
require "bundler"
require "multi_json"
#Bundler.require(:default, :development)

# For testing Hastur components, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "lib")

require "nodule/monkeypatch"
require "nodule/topology"
require "nodule/process"

HASTUR_ROOT = File.join(File.dirname(__FILE__), "..", "..")

HASTUR_ROUTER_BIN="#{HASTUR_ROOT}/infrastructure/hastur-router.rb"
HASTUR_CLIENT_BIN="#{HASTUR_ROOT}/bin/hastur-client.rb"
HASTUR_MSGTOOL_BIN="#{HASTUR_ROOT}/tools/msgtool.rb"

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

