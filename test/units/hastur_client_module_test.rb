#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'socket'

require 'hastur-server/client'
require 'hastur-server/message'

class TestClassHasturClientModule < MiniTest::Unit::TestCase
  UUID1 = SecureRandom.uuid
  UUID2 = SecureRandom.uuid
  ROUTER_URI = "ipc:///tmp/router"

  def test_client_module
    client = begin
      Hastur::Client.new(
        :uuid         => UUID1,
        :routers      => [ ROUTER_URI ],
        :port         => 20005,
        :heartbeat    => 5,
        :ack_interval => 1,
      )
    rescue
      # add fail
    end

    refute_nil client, "client instantiation failed"
  end
end

