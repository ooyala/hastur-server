#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/cassandra'
require 'multi_json'
require 'hastur'

class BringUpTest < Test::Unit::TestCase
  def setup
    set_test_alarm
    sinatra_ready = false
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :cyanio       => Nodule::Console.new(:fg => :cyan),
      :yellowio     => Nodule::Console.new(:fg => :yellow),
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
      :cass_sink => Nodule::Process.new(
        HASTUR_CASS_SINK_BIN,
        '--sinks', :stat, :event, :heartbeat, :registration, :log, :error,
        '--cassandra', :cassandra,
        :verbose => :cyanio, :stderr => :redio, :stdout => :yellowio
      ),
    )

    @topology.start :cassandra
    create_all_column_families(@topology[:cassandra]) # helper

    @topology.start_all
    sleep 0.01 until sinatra_ready
  end

  def teardown
    @topology.stop :cass_sink
    @topology.stop_all
  end

  def test_bring_up
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
