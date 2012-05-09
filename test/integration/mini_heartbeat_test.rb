#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "minitest/autorun"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'

class MiniHeartbeatTest < MiniTest::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
      :agent1unix   => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:bind => ZMQ::ROUTER, :uri => :gen, :reader => :capture, :limit => 2),
      :agent1svc    => Nodule::Process.new(
        HASTUR_AGENT_BIN, '--uuid', A1UUID, '--heartbeat', 1, '--router', :router,
        '--port', HASTUR_UDP_PORT,
        '--no-agent-stats', '--no-proc-stats',
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
    )

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_mini_heartbeat
    # wait for some messages to flow
    @topology[:router].require_read_count 2, test_timeout(15)
    messages = @topology[:router].output

    # work with raw messages for now
    payloads  = messages.map { |m| MultiJson.load(m[-1]) }
    heartbeat_payloads = payloads.select { |p| "name" == "hastur.agent.heartbeat" }
    envelopes = messages.map { |m| m[-2].unpack("H*") }

    assert_equal 2, messages.count, "Should have exactly two captured messages"
    assert_kind_of Array, messages[0], "messages should be 2-level arrays"
    assert_kind_of Array, messages[1], "messages should be 2-level arrays"

    # We receive on a ROUTER socket, so the socket ID is prepended before the
    # envelope and payload.
    assert_equal 3, messages[0].count, "each message should have 3 parts"
    assert_equal 3, messages[1].count, "each message should have 3 parts"

    a1uuid = A1UUID.gsub('-', '')
    assert envelopes.flatten.any? { |e| e.include?(a1uuid) }, "No envelope contains agent 1's UUID"
  end
end
