#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'

class MiniHeartbeatTest < Test::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
      :client1unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:bind => ZMQ::ROUTER, :uri => :gen, :reader => :capture, :limit => 2),

      :client1svc   => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C1UUID, '--heartbeat', 1, '--router', :router,
        '--port', HASTUR_UDP_PORT,
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
    sleep 3

    messages = @topology[:router].output

    # work with raw messages for now
    payloads  = messages.map { |m| MultiJson.decode(m[-1]) }
    heartbeat_payloads = payloads.fuzzy_filter("name" => "hastur.client.heartbeat")
    envelopes = messages.map { |m| m[-2].unpack("H*") }

    assert_equal 2, messages.count, "Should have exactly two captured messages"
    assert_kind_of Array, messages[0], "messages should be 2-level arrays"
    assert_kind_of Array, messages[1], "messages should be 2-level arrays"

    # We receive on a ROUTER socket, so the socket ID is prepended before the
    # envelope and payload.
    assert_equal 3, messages[0].count, "each message should have 3 parts"
    assert_equal 3, messages[1].count, "each message should have 3 parts"

    c1uuid = C1UUID.gsub('-', '')
    assert envelopes.flatten.any? { |e| e.include?(c1uuid) }, "No envelope contains client 1's UUID"
  end
end

