#!/usr/bin/env ruby
require_relative "../test_helper"

require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'socket'

require 'hastur-server/service/agent'
require 'hastur-server/message'

class TestClassHasturAgentModule < MiniTest::Unit::TestCase
  UUID1 = SecureRandom.uuid
  UUID2 = SecureRandom.uuid
  ROUTER_URI = "ipc:///tmp/router"

  def test_agent_module
    agent = begin
      Hastur::Service::Agent.new(
        :uuid         => UUID1,
        :routers      => [ ROUTER_URI ],
        :port         => 20005,
        :heartbeat    => 5,
        :ack_interval => 1,
      )
    rescue
      # add fail
    end

    refute_nil agent, "agent instantiation failed"
  end
end

