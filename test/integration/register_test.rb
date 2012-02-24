#!/usr/bin/env ruby

$LOAD_PATH.unshift File.dirname(__FILE__)

require "test/unit"
require "integration_test_helper"

require "nodule/topology"
require "nodule/process"
require "nodule/console"
require "nodule/unixsocket"
require "nodule/zeromq"
require "multi_json"

class RegisterTest < Test::Unit::TestCase

  def test_register
    sleep 1

    messages = @topology[:register].output
    payloads = messages.map { |m| MultiJson.decode(m[-1]) }

    assert_equal(1, payloads.count)
    assert_equal("register_client", payloads[0]["_route"])
  end

  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :client1unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :heartbeat    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :register     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => 1),
      :notification => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :plugin_exec  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),

      :client1svc   => Nodule::Process.new(
        HASTUR_CLIENT_BIN, "--uuid", C1UUID, "--router", :router, :stdout => :greenio, :stderr => :redio
      ),

      :routersvc    => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        "--uuid",         R1UUID,
        "--heartbeat",    :heartbeat,
        "--register",     :register,
        "--notification", :notification,
        "--stat",         :stat,
        "--log",          :log,
        "--error",        :error,
        "--router",       :router,
        "--plugin-exec",  :plugin_exec,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end
end

