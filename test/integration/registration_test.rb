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

class RegistrationTest < Test::Unit::TestCase

  def test_registration
    sleep 2

    messages = @topology[:registration].output
    payloads = messages.map { |m| MultiJson.decode(m[-1]) }

    assert_equal(1, payloads.count)
    assert_equal("client", payloads[0]["type"])
  end

  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :client1unix  => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :registration => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture,
                                          :limit => 5, :stdout => :greenio),
      :heartbeat    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen, :reader => :drain),

      :client1svc   => Nodule::Process.new(
        HASTUR_CLIENT_BIN, "--uuid", C1UUID, "--router", :router, :stdout => :greenio, :stderr => :redio
      ),

      :routersvc    => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        "--uuid",         R1UUID,
        "--heartbeat",    :heartbeat,
        "--registration", :registration,
        "--event",        :event,
        "--stat",         :stat,
        "--log",          :log,
        "--error",        :error,
        "--router",       :router,
        "--direct",       :direct,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end
end

