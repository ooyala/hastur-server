#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/zeromq'
require 'multi_json'
require 'hastur-server/message'

class AckTest < Test::Unit::TestCase
  EVENT_REPLAYS = 10

  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :yellow        => Nodule::Console.new(:fg => :yellow),
      :agent         => Nodule::ZeroMQ.new(:connect => ZMQ::DEALER, :uri => :gen, :reader => :capture),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :yellow),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :routersvc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--hwm',           10000,
        '--router',        :agent,
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

    # emulate an agent heartbeat and wait for it to go all the way through to
    # make sure we're ready to go
    @agent = @topology[:agent].socket
    hb = Hastur::Message::HB::Agent.new(:payload => "{}", :from => C1UUID)
    hb.send @agent
    @topology[:heartbeat].require_read_count 1
  end

  def teardown
    @topology.stop_all
  end

  def test_event_ack
    hb = Hastur::Message::Event.new(:payload => "{}", :from => C1UUID)

    EVENT_REPLAYS.times do
      rc = hb.send @agent
      assert ZMQ::Util.resultcode_ok? rc
      sleep 0.1
    end

    @topology[:agent].require_read_count EVENT_REPLAYS, 10

    assert_equal EVENT_REPLAYS, @topology[:agent].output.count, "should have gotten #{EVENT_REPLAYS} messages"
  end
end
