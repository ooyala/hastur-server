#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/cassandra'
require 'multi_json'
require 'open-uri'
require 'hastur'

class QueryServerTest < Test::Unit::TestCase
  def setup
    set_test_alarm(100)
    sinatra_ready = false
    sinatra_ready_proc = proc do |line|
      sinatra_ready = true if line =~ /== Sinatra.* has taken the stage/
    end
    @sinatra_port = Nodule::Util.random_tcp_port

    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :client1unix  => Nodule::UnixSocket.new,
      :client2unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat    => Nodule::ZeroMQ.new(:uri => :gen),
      :registration => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :rawdata      => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::REP,  :uri => :gen),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :cassandra    => Nodule::Cassandra.new(:keyspace => "Hastur",
        :stderr => :redio, :verbose => :cyanio, #:stdout => :greenio,
      ),
      :query_server => Nodule::Process.new(HASTUR_QUERY_SERVER_BIN,
        '--cassandra', :cassandra, '--port', @sinatra_port.to_s,
        :stdout => :greenio, :stderr => [sinatra_ready_proc, :greenio], :verbose => :cyanio
      ),

      :client1svc   => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C1UUID, '--heartbeat', 1, '--router', :router, '--unix', :client1unix,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),

      :client2svc => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C2UUID, '--heartbeat', 1, '--router', :router, '--unix', :client2unix,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
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
        '--rawdata',      :rawdata,
        '--control',      :control,
        '--router',       :router,
        '--direct',       :direct,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),

      :cass_sink => Nodule::Process.new(HASTUR_CASS_SINK_BIN,
        '--sinks',     :heartbeat,
        '--acks-to',   :direct,
        '--cassandra', :cassandra,
        :verbose => :redio, :stderr => :redio, :stdout => :yellowio
      ),
    )

    # start cassandra first and set up the CF's before bringing anything else up
    @topology.start :cassandra

    # this will also issue the CREATE KEYSPACE command
    create_all_column_families(@topology[:cassandra]) # helper

    @topology.start_all
    sleep 0.01 until sinatra_ready
  end

  def teardown
    # stop the cassandra sink before cassandra so it doesn't blow up
    @topology.stop :cass_sink
    @topology.stop_all
  end

  def test_query_server
    wait_for_cassandra_rows(@topology[:cassandra].client, "HeartbeatArchive", 4, 10)

    # Query from 10 minutes ago to 10 minutes from now, just to grab everything
    start_ts = Hastur.timestamp(Time.now.to_i - 600)
    end_ts = Hastur.timestamp(Time.now.to_i + 600)

    url1 = "http://127.0.0.1:#{@sinatra_port}/data/heartbeat/json?uuid=#{C1UUID}&start=#{start_ts}&end=#{end_ts}"
    c1_html = open(url1).read
    assert c1_html.length > 10, "got at least 10 bytes of data for the client 1 heartbeat query"
    assert c1_html.length < 4096, "got no more than 4096 bytes of data for the client 1 heartbeat query"
    assert c1_html.match(/^\s*{.*}\s*$/), "looks like JSON"
    c1_messages = MultiJson.decode c1_html

    # always run two tests - we've seen cases where the first works but the second doesn't
    url2 = "http://127.0.0.1:#{@sinatra_port}/data/heartbeat/values?uuid=#{C2UUID}&start=#{start_ts}&end=#{end_ts}"
    c2_html = open(url2).read
    assert c2_html.length > 10, "got at least 10 bytes of data for the client 2 heartbeat query"
    assert c2_html.length < 4096, "got no more than 4096 bytes of data for the client 2 heartbeat query"
    assert c2_html.match(/^\s*{.*}\s*$/), "looks like JSON"
    c2_messages = MultiJson.decode c2_html
  end
end
