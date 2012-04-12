#!/usr/bin/env ruby

require 'multi_json'
require "test/unit"

require_relative "./integration_test_helper"

require 'hastur'
require 'hastur-server/message'

require 'nodule/cassandra'
require 'nodule/console'
require 'nodule/process'
require 'nodule/topology'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/util'

class BringRouterDownTest < Test::Unit::TestCase

public

  def setup
    set_test_alarm(100) # helper

    @client_udp_port1 = Nodule::Util.random_udp_port
    @client_udp_port2 = Nodule::Util.random_udp_port
    @heartbeat_client1 = "heartbeat-client1"
    @heartbeat_client2 = "heartbeat-client2"

    sinatra_ready = false
    sinatra_ready_proc = proc do |line|
      sinatra_ready = true if line =~ /== Sinatra.* has taken the stage/
    end

    @sinatra_port = Nodule::Util.random_tcp_port

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :router1       => Nodule::ZeroMQ.new(:uri => :gen),
      :router2       => Nodule::ZeroMQ.new(:uri => :gen),
      :registration1  => Nodule::ZeroMQ.new(:uri => :gen),
      :registration2  => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat1     => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat2     => Nodule::ZeroMQ.new(:uri => :gen),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen, :reader => :drain),
      :cassandra     => Nodule::Cassandra.new( :keyspace => "Hastur", :verbose => :greenio ),
      :router1svc    => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :router1,
        '--event',         :event,
        '--heartbeat',     :heartbeat1,
        '--registration',  :registration1,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--rawdata',       :rawdata,
        '--direct',        :direct,
        '--control',       :control,
        '--hwm',           100,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :router2svc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R2UUID,
        '--router',        :router2,
        '--event',         :event,
        '--heartbeat',     :heartbeat2,
        '--registration',  :registration2,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--rawdata',       :rawdata,
        '--direct',        :direct,
        '--control',       :control,
        '--hwm',           100,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :client1svc    => Nodule::Process.new(
        HASTUR_CLIENT_BIN,
        '--uuid',         C1UUID,
        '--router',       :router2,
        '--router',       :router1,
        '--ack-timeout',  1,
        '--heartbeat',    300,
        '--port',         @client_udp_port1,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :client2svc    => Nodule::Process.new(
        HASTUR_CLIENT_BIN,
        '--uuid',         C2UUID,
        '--router',       :router2,
        '--router',       :router1,
        '--ack-timeout',  1,
        '--heartbeat',    300,
        '--port',         @client_udp_port2,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :cass_sink1     => Nodule::Process.new(
        HASTUR_CASS_SINK_BIN,
        '--sinks',       :heartbeat1, :registration1, :heartbeat2, :registration2,
        '--cassandra',   :cassandra,
        '--acks-to',     :direct,
        '--hwm',         100,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
      :query_server   => Nodule::Process.new(HASTUR_QUERY_SERVER_BIN,
        '--cassandra', :cassandra, '--port', @sinatra_port.to_s,
        :stdout => :greenio, :stderr => [sinatra_ready_proc, :greenio], :verbose => :cyanio
      ),
    )
    # start cassandra
    @topology.start :cassandra
    @topology.start :router1svc
    @topology.start :router2svc
    create_all_column_families(@topology[:cassandra]) # helper
    # start everything else but the scheduler
    @topology.start_all
    # wait for the row to show up in Cassandra
    client = @topology[:cassandra].client
    wait_for_cassandra_rows(client, "RegAgentArchive", 1, 60) do
      flunk "Gave up waiting for registrations in cassandra."
    end

    sleep 0.01 until sinatra_ready
  end

  def teardown
    @topology.stop_all
  end

  def test_router_restart
    # send heartbeat to both clients
    send_2_heartbeat(@client_udp_port1, @client_udp_port2, @heartbeat_client1, @heartbeat_client2)

    # ensure that both heartbeats were received
    ensure_heartbeats(true, @heartbeat_client1, @heartbeat_client2, 1, 1, @sinatra_port)

    # shut a client down
    @topology.stop :router1svc
#    sleep 5

    # resend heartbeats to both clients
    send_2_heartbeat(@client_udp_port1, @client_udp_port2, @heartbeat_client1, @heartbeat_client2)

    sleep 2

    # ensure that only one heartbeat was received
    ensure_heartbeats(true, @heartbeat_client1, @heartbeat_client2, 2, 2, @sinatra_port)

    # start a client
    @topology.start :router1svc
    ENV["IS_JENKINS"].nil? ? (sleep 5) : (sleep 15)

    # resend heartbeats to both clients
    send_2_heartbeat(@client_udp_port1, @client_udp_port2, @heartbeat_client1, @heartbeat_client2)

    # ensure that both heartbeats were received
    ensure_heartbeats(true, @heartbeat_client1, @heartbeat_client2, 3, 3, @sinatra_port)
  end
end
