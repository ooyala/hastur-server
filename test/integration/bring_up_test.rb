#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "minitest/autorun"
require 'nodule'
require 'nodule/alarm'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/cassandra'
require 'hastur/api'
require 'open-uri'

class BringUpTest < MiniTest::Unit::TestCase
  def setup
    sinatra_ready = false
    sinatra_ready_proc = proc do |line|
      sinatra_ready = true if line =~ /== Sinatra.* has taken the stage/
    end
    @sinatra_port = Nodule::Util.random_tcp_port
    @agent1_port = Nodule::Util.random_udp_port
    @agent2_port = Nodule::Util.random_udp_port
    @topology = Nodule::Topology.new(
      :alarm        => Nodule::Alarm.new(:timeout => test_timeout(100)),
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat    => Nodule::ZeroMQ.new(:uri => :gen),
      :registration => Nodule::ZeroMQ.new(:uri => :gen),
      :stat         => Nodule::ZeroMQ.new(:uri => :gen),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::REP,  :uri => :gen),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :cassandra    => Nodule::Cassandra.new(:keyspace => "hastur",
        :stderr => :redio, :verbose => :cyanio, #:stdout => :greenio,
      ),
      :query_server => Nodule::Process.new(HASTUR_QUERY_SERVER_BIN,
        '--cassandra', :cassandra, '--port', @sinatra_port.to_s,
        :stdout => :greenio, :stderr => [sinatra_ready_proc, :greenio], :verbose => :cyanio
      ),
      :agent1svc => Nodule::Process.new(
        HASTUR_AGENT_BIN, '--uuid', A1UUID, '--heartbeat', 1, '--router', :router, '--port', @agent1_port,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :agent2svc => Nodule::Process.new(
        HASTUR_AGENT_BIN, '--uuid', A2UUID, '--heartbeat', 1, '--router', :router, '--port', @agent2_port,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :router1svc => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',         R1UUID,
        '--heartbeat',    :heartbeat,
        '--registration', :registration,
        '--event',        :event,
        '--stat',         :stat,
        '--log',          :log,
        '--error',        :error,
        '--control',      :control,
        '--router',       :router,
        '--direct',       :direct,
        '--hwm',          10,   # Set HWM so this doesn't 'clog'
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
      :router2svc => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',         R2UUID,
        '--heartbeat',    :heartbeat,
        '--registration', :registration,
        '--event',        :event,
        '--stat',         :stat,
        '--log',          :log,
        '--error',        :error,
        '--control',      :control,
        '--router',       :router,
        '--direct',       :direct,
        '--hwm',          10,   # Set HWM so this doesn't 'clog'
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
      :cass_sink1 => Nodule::Process.new(
        HASTUR_CASS_SINK_BIN,
        '--sinks', :stat, :heartbeat, :registration,
        '--acks-to', :direct,
        '--cassandra', :cassandra,
        :verbose => :cyanio, :stderr => :redio, :stdout => :yellowio
      ),
      :cass_sink2 => Nodule::Process.new(
        HASTUR_CASS_SINK_BIN,
        '--sinks', :stat, :heartbeat, :registration,
        '--acks-to', :direct,
        '--cassandra', :cassandra,
        :verbose => :cyanio, :stderr => :redio, :stdout => :yellowio
      ),
    )

    @topology.start :cassandra
    create_all_column_families(@topology[:cassandra]) # helper

    @topology.start_all_but :router2svc, :cass_sink2, :agent2svc
    sleep 0.01 until sinatra_ready

    # wait for the row to show up in Cassandra
    wait_for_cassandra_rows(@topology[:cassandra].client, "reg_agent_archive", 1, 5) do
      flunk "Gave up waiting for registration in cassandra."
    end
  end

  def teardown
    @topology.stop :cass_sink1
    @topology.stop :cass_sink2
    @topology.stop_all
  end

  def test_bring_up
    # Query from 10 minutes ago to 10 minutes from now, just to grab everything
    start_ts = Hastur.timestamp(Time.now.to_i - 600)
    end_ts = Hastur.timestamp(Time.now.to_i + 600)

    # Start up and test second agent
    @topology.start :agent2svc
    sleep 1

    url1 = "http://127.0.0.1:#{@sinatra_port}/data/registration/json?uuid=#{A1UUID}&start=#{start_ts}&end=#{end_ts}"
    a1_messages = open(url1).read
    assert_json_not_empty a1_messages

    url2 = "http://127.0.0.1:#{@sinatra_port}/data/registration/json?uuid=#{A2UUID}&start=#{start_ts}&end=#{end_ts}"
    a2_messages = open(url2).read
    assert_json_not_empty a2_messages

    # Start up and test second router
    @topology.start :router2svc

    sleep 1

    Hastur.udp_port = @agent1_port
    Hastur.counter("Agent1.countme")

    Hastur.udp_port = @agent2_port
    Hastur.counter("Agent2.countme")

    sleep 1

    url1 = "http://127.0.0.1:#{@sinatra_port}/data/stat/values?uuid=#{A1UUID}&start=#{start_ts}&end=#{end_ts}"
    a1_messages = open(url1).read
    assert_json_not_empty a1_messages

    url2 = "http://127.0.0.1:#{@sinatra_port}/data/stat/values?uuid=#{A2UUID}&start=#{start_ts}&end=#{end_ts}"
    a2_messages = open(url2).read
    assert_json_not_empty a2_messages

    # Start up and test second sink
    @topology.start :cass_sink2

    sleep 1

    Hastur.udp_port = @agent1_port
    Hastur.counter("Agent1.second_countme")

    Hastur.udp_port = @agent2_port
    Hastur.counter("Agent2.second_countme")

    sleep 1

    url1 = "http://127.0.0.1:#{@sinatra_port}/data/stat/values?uuid=#{A1UUID}&start=#{start_ts}&end=#{end_ts}"
    a1_messages = open(url1).read
    refute_nil a1_messages =~ /second_countme/

    url2 = "http://127.0.0.1:#{@sinatra_port}/data/stat/values?uuid=#{A2UUID}&start=#{start_ts}&end=#{end_ts}"
    a2_messages = open(url2).read
    refute_nil a2_messages =~ /second_countme/
  end
end
