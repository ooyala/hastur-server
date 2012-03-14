#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'

class HeartbeatTest < Test::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
      :client1unix  => Nodule::UnixSocket.new,
      :client2unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => 4),
      :registration => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),

      :client1svc   => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C1UUID, '--heartbeat', 1, '--router', :router, '--unix', :client1unix,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),

      :client2svc => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C2UUID, '--heartbeat', 1, '--router', :router, '--unix', :client2unix,
        :stdout => :greenio, :stderr => :redio, :verbose => :yellowio,
      ),

      :routersvc => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',         R1UUID,
        '--heartbeat',    :heartbeat,
        '--registration', :registration,
        '--event',        :event,
        '--stat',         :stat,
        '--log',          :log,
        '--error',        :error,
        '--router',       :router,
        '--direct',       :direct,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_heartbeat
    # wait for some messages to flow
    sleep 3

    messages = @topology[:heartbeat].output
    # work with raw messages for now
    payloads  = messages.map { |m| MultiJson.decode(m[-1]) }
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
    assert_equal(payloads.count, payloads.fuzzy_filter("value" => Fixnum).count)
    assert_equal(payloads.count, payloads.fuzzy_filter("name" => "hastur.client.heartbeat").count)

    c1uuid = C1UUID.gsub('-', '')
    c2uuid = C2UUID.gsub('-', '')
    assert envelopes.flatten.any? { |e| e.include?(c1uuid) }, "No envelope contains client 1's UUID"
    assert envelopes.flatten.any? { |e| e.include?(c2uuid) }, "No envelope contains client 2's UUID"
  end
end

