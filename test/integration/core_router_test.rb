#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "minitest/autorun"
require 'nodule'
require 'nodule/zeromq'
require 'nodule/cassandra'
require 'multi_json'
require 'hastur-server/message'
require 'hastur-server/mock/nodule_agent'

class CoreRouterTest < MiniTest::Unit::TestCase
  EVENT_REPLAYS = 10

  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :yellow        => Nodule::Console.new(:fg => :yellow),
      :mock_agent    => Hastur::Mock::NoduleAgent.new(:reader => :capture),
      :firehose      => Nodule::ZeroMQ.new(:connect => ZMQ::SUB, :uri => :gen, :reader => :capture),
      :return        => Nodule::ZeroMQ.new(:uri => :gen),
      :cassandra     => Nodule::Cassandra.new(:keyspace => "Hastur"),
      :coresvc       => Nodule::Process.new(
        HASTUR_CORE_BIN,
        '--uuid',          R1UUID,
        '--router',        :mock_agent,
        '--firehose',      :firehose,
        '--return',        :return,
        '--cassandra',     :cassandra,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      )
    )

    # start cassandra first and set up the CF's before bringing anything else up
    @topology.start :cassandra
    create_all_column_families(@topology[:cassandra]) # helper

    @topology.start_all

    # subscribe to everything
    @topology[:firehose].subscribe ""

    @topology[:mock_agent].heartbeat

    sleep 1
    @topology[:firehose].require_read_count 1, 30
  end

  def teardown
    @topology.stop_all
  end

  def test_core_router_ack
    @topology[:mock_agent].heartbeat
    @topology[:firehose].require_read_count 2, 30

    # why does this take so long, even on my fast box?
    # TODO: figure out why this is timing out even on nice machines (al, 2012-04-12)
    #start = Time.now
    #assert wait_for_cassandra_rows(@topology[:cassandra].client, "HBProcessArchive", 2, 60)
    #STDERR.puts "Took #{Time.now - start} seconds for the rows to show up in Cassandra."

    # check ack flow
    event = Hastur::Message::Event.new(:payload => EVENT_JSON, :from => A1UUID)
    rc = event.send @topology[:mock_agent].socket
    assert ZMQ::Util.resultcode_ok?(rc)

    sleep 1

    @topology[:mock_agent].require_read_count 1

    assert true
  end
end
