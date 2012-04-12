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
      :mock_agent    => Hastur::Mock::NoduleAgent.new(:reader => :capture),
      :firehose      => Nodule::ZeroMQ.new(:connect => ZMQ::SUB, :uri => :gen, :reader => :capture),
      :return        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :routersvc     => Nodule::Process.new(
        HASTUR_CORE_BIN,
        '--uuid',          R1UUID,
        '--router',        :mock_agent,
        '--firehose',      :firehose,
        '--return',        :return,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      )
    )

    @topology.start_all
    @topology[:firehose].subscribe ""

    @topology[:mock_agent].heartbeat
    @topology[:firehose].require_read_count 1
  end

  def teardown
    @topology.stop_all
  end

  def test_event_ack
    @topology[:mock_agent].heartbeat
    @topology[:firehose].require_read_count 2

    # check ack flow
    event = Hastur::Message::Event.new(:payload => EVENT_JSON, :from => A1UUID)
    rc = event.send @topology[:mock_agent].socket
    assert ZMQ::Util.resultcode_ok?(rc)

    @topology[:mock_agent].require_read_count 1

    assert true
  end
end
