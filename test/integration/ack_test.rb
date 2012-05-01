#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/zeromq'
require 'multi_json'
require 'hastur-server/message'
require 'hastur-server/mock/nodule_agent'

class AckTest < Test::Unit::TestCase
  EVENT_REPLAYS = 10

  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :yellow        => Nodule::Console.new(:fg => :yellow),
      :mock_agent    => Hastur::Mock::NoduleAgent.new,
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :routersvc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :mock_agent,
        '--event',         :event,
        '--heartbeat',     :heartbeat,
        '--registration',  :registration,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--direct',        :direct,
        '--control',       :control,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      )
    )

    # set a proc on event messages that always acks on receipt
    @topology[:event].add_reader do |messages|
      e = Hastur::Envelope.parse(messages[-2])
      assert_not_nil e
      ack = e.to_ack
      rc = ack.send @topology[:direct].socket
      assert rc > -1, "sending an ack created from the envelope of the message"
    end

    @topology.start_all
    sleep 0.5

    @topology[:mock_agent].heartbeat
    @topology[:heartbeat].require_read_count 1
  end

  def teardown
    @topology.stop_all
  end

  def test_event_ack
    event = Hastur::Message::Event.new(:payload => "{}", :from => A1UUID)

    EVENT_REPLAYS.times do
      rc = event.send @topology[:mock_agent].socket
      assert ZMQ::Util.resultcode_ok? rc
      sleep 0.2
    end

    @topology[:mock_agent].require_read_count EVENT_REPLAYS, 30

    assert_equal EVENT_REPLAYS, @topology[:mock_agent].output.count, "should have gotten #{EVENT_REPLAYS} messages"
  end
end
