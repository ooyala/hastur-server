#!/usr/bin/env ruby

require "rubygems"
require "test/unit"
require_relative "./integration_test_helper"

require 'nodule/topology'
require 'nodule/process'
require 'nodule/console'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'

require 'hastur'

CASSANDRA_BIN = "#{ENV['HOME']}/apache-cassandra-1.0.7/bin/cassandra"

exit 0 unless File.exist?(CASSANDRA_BIN)

class QueryServerTest < Test::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :client1unix  => Nodule::UnixSocket.new,
      :client2unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture),
      :registration => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :statsvc      => Nodule::Process.new(HASTUR_CASS_SINK_BIN, "--routers", :stat, :stderr => :redio),
      :cassandra    => Nodule::Process.new(CASSANDRA_BIN, "-f"),
      :query_server => Nodule::Process.new(HASTUR_QUERY_SERVER_BIN, "--", "-p", "4177"),

      :client1svc   => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C1UUID, '--heartbeat', 1, '--router', :router, '--unix', :client1unix,
        :stdout => :greenio, :stderr => :redio,
      ),

      :client2svc => Nodule::Process.new(
        HASTUR_CLIENT_BIN, '--uuid', C2UUID, '--heartbeat', 1, '--router', :router, '--unix', :client2unix,
        :stdout => :greenio, :stderr => :redio,
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
        '--hwm',          10,   # Set HWM so this doesn't 'clog'
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_query_server
    # TODO: some of the tests below may have to change, since the clients will continue to send heartbeats
    # with this method of sync.
    @topology[:heartbeat].require_read_count 4, 5

    messages = @topology[:heartbeat].output
    # First, check messages
    payloads  = messages.map { |m| MultiJson.decode(m[-1]) }
    envelopes = messages.map { |m| m[-2].unpack("H*") }

    STDERR.puts "Heartbeat message(s): #{messages.inspect}"

    # Query from 10 minutes ago to 10 minutes from now, just to grab everything
    start_ts = Hastur.timestamp(Time.now.to_i - 600)
    end_ts = Hastur.timestamp(Time.now.to_i + 600)

    c1_messages = MultiJson.decode `curl http://localhost:4177/data/heartbeat/json?uuid=#{C1UUID}\\\&start=#{start_ts}\\\&end=#{end_ts}`
    c2_messages = MultiJson.decode `curl http://localhost:4177/data/heartbeat/values?uuid=#{C2UUID}\\\&start=#{start_ts}\\\&end=#{end_ts}`

    STDERR.puts "Client 1 messages: #{c1_messages.inspect}"
    STDERR.puts "Client 2 messages: #{c2_messages.inspect}"
  end
end
