#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "minitest/autorun"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'

class HeartbeatTest < MiniTest::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :yellowio      => Nodule::Console.new(:fg => :yellow),
      :agent1unix    => Nodule::UnixSocket.new,
      :agent2unix    => Nodule::UnixSocket.new,
      :core_router   => Nodule::ZeroMQ.new(:uri => :gen),
      :core_return   => Nodule::ZeroMQ.new(:uri => :gen),
      :core_firehose => Nodule::ZeroMQ.new(:uri => :gen, :reader => :capture),

      :agent1svc   => Nodule::Process.new(
        HASTUR_AGENT_BIN, '--uuid', A1UUID, '--heartbeat', 1, '--router', :core_router, '--unix', :agent1unix,
        '--port', HASTUR_UDP_PORT,
        '--no-agent-stats', '--no-proc-stats',
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),

      :agent2svc => Nodule::Process.new(
        HASTUR_AGENT_BIN, '--uuid', A2UUID, '--heartbeat', 1, '--router', :core_router, '--unix', :agent2unix,
        '--port', Nodule::Util.random_udp_port,
        '--no-agent-stats', '--no-proc-stats',
        :stdout => :greenio, :stderr => :redio, :verbose => :yellowio,
      ),

      :routersvc => Nodule::Process.new(
        HASTUR_CORE_BIN,
        '--uuid',         R1UUID,
        '--router',       :core_router,
        '--return',       :core_return,
        '--firehose',     :core_firehose,
        '--no-sink',
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def only_agent_heartbeats(msgs)
    msgs.select do |m|
      MultiJson.load(m[-1])["name"] == "hastur.agent.heartbeat"
    end
  end

  def test_heartbeat
    # wait for some messages to flow
    @topology[:core_firehose].read_until(:max_sleep => test_timeout(20), :sleep_by => 0.5) do
      heartbeats = only_agent_heartbeats(@topology[:core_firehose].output)
      heartbeats.count >= 4
    end

    messages = only_agent_heartbeats(@topology[:core_firehose].output)
    # work with raw messages for now
    payloads  = messages.map { |m| MultiJson.load(m[-1]) }
    envelopes = messages.map { |m| m[-2].unpack("H*") }
    #puts messages.flatten.map { |i| i.unpack("H*") }

    assert_equal 4, messages.count, "Should have exactly four captured messages"
    assert_kind_of Array, messages[0], "messages should be 2-level arrays"
    assert_kind_of Array, messages[1], "messages should be 2-level arrays"
    assert_kind_of Array, messages[2], "messages should be 2-level arrays"
    assert_kind_of Array, messages[3], "messages should be 2-level arrays"
    assert_equal 2, messages[0].count, "each message should have 2 parts"
    assert_equal 2, messages[1].count, "each message should have 2 parts"

    # verify that the messages on the heartbeat shims are heartbeat messages
    assert_equal(payloads.count, payloads.select { |p| p["value"].is_a? Fixnum }.count)
    assert_equal(payloads.count, payloads.select { |p| p["name"] == "hastur.agent.heartbeat" }.count)

    a1uuid = A1UUID.gsub('-', '')
    a2uuid = A2UUID.gsub('-', '')
    assert envelopes.flatten.any? { |e| e.include?(a1uuid) }, "No envelope contains agent 1's UUID"
    assert envelopes.flatten.any? { |e| e.include?(a2uuid) }, "No envelope contains agent 2's UUID"
  end
end
