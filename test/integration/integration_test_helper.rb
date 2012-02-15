require "rubygems"
require "bundler"
require "multi_json"
#Bundler.require(:default, :development)

# For testing Hastur components, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "lib")

require "hastur/monkeypatch"
require "hastur/test/topology"
require "hastur/test/process"

HASTUR_ROOT = File.join(File.dirname(__FILE__), "..", "..")

HASTUR_ROUTER_BIN="#{HASTUR_ROOT}/infrastructure/hastur-router.rb"
HASTUR_CLIENT_BIN="#{HASTUR_ROOT}/bin/hastur-client-v2.rb"
HASTUR_MSGTOOL_BIN="#{HASTUR_ROOT}/tools/msgtool.rb"

