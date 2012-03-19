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

class FullPluginTest < Test::Unit::TestCase
  def setup
    set_test_alarm(30) # helper

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :registration  => Nodule::ZeroMQ.new(:uri => :gen),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen, :reader => :drain),
      :cassandra     => Nodule::Cassandra.new( :keyspace => "Hastur", :verbose => :greenio ),
      :routersvc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :router,
        '--event',         :event,
        '--heartbeat',     :heartbeat,
        '--registration',  :registration,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--rawdata',       :rawdata,
        '--direct',        :direct,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :client1svc    => Nodule::Process.new(
        HASTUR_CLIENT_BIN,
        '--uuid',         C1UUID,
        '--router',       :router,
        '--ack-timeout',  1,
        '--heartbeat',    300,
        '--port',         HASTUR_UDP_PORT,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :regsvc       => Nodule::Process.new(
        HASTUR_CASS_SINK_BIN,
        '--sinks',       :registration,
        '--cassandra',   :cassandra,
        '--acks-to',     :direct,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
      :scheduler     => Nodule::Process.new(
        HASTUR_SCHEDULER,
        "--routers",     :direct,
        "--hosts",       :cassandra,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    # start cassandra
    @topology.start :cassandra
    create_all_column_families(@topology[:cassandra]) # helper

    # start everything else but the scheduler
    @topology.keys.each do |key|
      if key.to_s != "scheduler" && key.to_s != "cassandra"
        @topology.start key.to_sym
      end
    end
  end

  def teardown
    @topology.stop_all
  end

  def test_plugin
    client = @topology[:cassandra].client
    
    # wait for the row to show up in Cassandra
    wait_for_cassandra_rows(client, "RegistrationArchive", 1, 5) do
      flunk "Gave up waiting for registrations in cassandra."
    end

    # start the scheduler once we know cassandra is up and running
    @topology.start :scheduler

    @topology[:heartbeat].require_read_count 1, 1

    # make sure the cassandra schema is at least loaded
    hash = client.get(:RegistrationArchive, "kye")
    assert_not_nil hash
    assert_equal "Hastur", @topology[:cassandra].keyspace

    # should be one heartbeat before any plugins are registered
    heartbeat_msgs = @topology[:heartbeat].output
    assert_equal 1, heartbeat_msgs.size

    # register plugin
    Hastur.register_plugin("my.plugin.echo", "echo", "OK", :five_minutes)

    #sleep 10

    # the schedule should pick up the registration and start generating more heartbeats
    @topology[:heartbeat].require_read_count 2, 12 do
      flunk "Gave up waiting for the plugin's heartbeat to arrive"
    end
    heartbeat_msgs = @topology[:heartbeat].output
    assert_equal 2, heartbeat_msgs.size

    # verify that the result came back on the heartbeat socket
    heartbeat_msgs = @topology[:heartbeat].output
    heartbeat_payloads = heartbeat_msgs.map do |m|
      assert_equal 2, m.size
      MultiJson.decode m[1]
    end
    plugin_result = heartbeat_payloads.fuzzy_filter("name" => "my.plugin.echo")
    assert_equal 1, plugin_result.size
  end
end
