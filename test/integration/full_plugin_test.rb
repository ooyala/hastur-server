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

class FullPluginTest < Test::Unit::TestCase
  def setup
    set_test_alarm(30) # helper

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
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
        '--direct',        :direct,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :client1svc    => Nodule::Process.new(
        HASTUR_CLIENT_BIN,
        '--uuid',         C1UUID,
        '--router',       :router,
        '--ack-timeout',  1,
        '--heartbeat',    300,
        '--port',         8125,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :regsvc       => Nodule::Process.new(
        HASTUR_CASS_SINK_BIN,
        '--sinks',       :registration,
        '--cassandra',   :cassandra,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
      :scheduler     => Nodule::Process.new(
        HASTUR_SCHEDULER,
        "--routers",     :direct,
        "--hosts",       :cassandra,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @topology.start_all
    create_all_column_families(@topology[:cassandra].client) # helper
    sleep 5
  end

  def teardown
    @topology.stop_all
  end

  def test_plugin
    # make sure the cassandra schema is at least loaded
    client = @topology[:cassandra].client
    hash = client.get(:RegistrationArchive, "kye")
    assert_not_nil hash
    assert_equal "Hastur", @topology[:cassandra].keyspace

    # register plugin
    Hastur.register_plugin("my.plugin.echo", "echo", "OK", :five_minutes)

    # give time for the register_plugin message to make its way over
    sleep 2
    Hastur.register_plugin("my.plugin.echo", "echo", "OK", :five_minutes)

    heartbeat_msgs = @topology[:heartbeat].output
    assert_equal 1, heartbeat_msgs.size

    # give time for the scheduler to pick up the new registration
    sleep 10
    Hastur.register_plugin("my.plugin.echo", "echo", "OK", :five_minutes)
    sleep 1

    heartbeat_msgs = @topology[:heartbeat].output
    assert_equal 2, heartbeat_msgs.size

    # verify that the result came back on the heartbeat socket
    heartbeat_msgs = @topology[:heartbeat].output
    heartbeat_payloads = heartbeat_msgs.map do |m|
      assert_equal 2, m.size
      MultiJson.decode m[1]
    end
    plugin_result = heartbeat_payloads.fuzzy_filter("command" => "echo")
    assert_equal 1, plugin_result.size
  end
end
