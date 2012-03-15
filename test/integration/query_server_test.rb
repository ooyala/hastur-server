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
    sinatra_ready = false
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :client1unix  => Nodule::UnixSocket.new,
      :client2unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture),
      :registration => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :rawdata      => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::REP,  :uri => :gen),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :cassandra    => Nodule::Cassandra.new(:keyspace => "Hastur"),
      :query_server => Nodule::Process.new(HASTUR_QUERY_SERVER_BIN,
        '--cassandra', :cassandra,
        '--', '-p', '4177',
        :stdout => :greenio, :stderr => proc do |line|
          sinatra_ready = true if line =~ /== Sinatra.* has taken the stage/
        end,
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
        '--hwm',          10,   # Set HWM so this doesn't 'clog'
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
    # TODO: some of the tests below may have to change, since the clients will continue to send heartbeats
    # with this method of sync.
    @topology[:heartbeat].require_read_count 4, 10

    # Query from 10 minutes ago to 10 minutes from now, just to grab everything
    start_ts = Hastur.timestamp(Time.now.to_i - 600)
    end_ts = Hastur.timestamp(Time.now.to_i + 600)

    c1_html = open("http://localhost:4177/data/heartbeat/json?uuid=#{C1UUID}&start=#{start_ts}&end=#{end_ts}") do |f| f.read end
    assert c1_html.length > 10, "got at least 10 bytes of data for the client 1 heartbeat query"
    assert c1_html.length < 4096, "got no more than 4096 bytes of data for the client 1 heartbeat query"
    assert c1_html.match(/^\s*{.*}\s*$/), "looks like JSON"
    #c1_messages = MultiJson.decode c1_html

    # TODO: this still fails even though the first one succeeds, figure out why

    # always run two tests - we've seen cases where the first works but the second doesn't
    #c2_html = open("http://localhost:4177/data/heartbeat/values?uuid=#{C2UUID}&start=#{start_ts}&end=#{end_ts}") do |f| f.read end
    #assert c2_html.length > 10, "got at least 10 bytes of data for the client 2 heartbeat query"
    #assert c2_html.length < 4096, "got no more than 4096 bytes of data for the client 2 heartbeat query"
    #assert c2_html.match(/^\s*{.*}\s*$/), "looks like JSON"
    #c2_messages = MultiJson.decode c2_html
  end
end
