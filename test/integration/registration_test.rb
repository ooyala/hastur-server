#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require "nodule"
require "nodule/unixsocket"
require "nodule/zeromq"
require "multi_json"

class RegistrationTest < Test::Unit::TestCase

  def test_registration
    @topology[:registration].require_read_count 1, 10

    messages = @topology[:registration].output
    assert_equal 1, messages.count

    payload = MultiJson.load messages[0][-1] rescue nil
    assert_not_nil payload
    assert_kind_of Hash, payload

    assert_equal payload["from"], "11111111-2222-3333-4444-555555555555"
    assert_equal payload["source"], "Hastur::Service::Agent"
  end

  def setup
    @topology = Nodule::Topology.new(
      :greenio      => Nodule::Console.new(:fg => :green),
      :redio        => Nodule::Console.new(:fg => :red),
      :agent1unix   => Nodule::UnixSocket.new,
      :router       => Nodule::ZeroMQ.new(:uri => :gen),
      :registration => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => 1),
      :heartbeat    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :event        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :direct       => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),

      :agent1svc   => Nodule::Process.new(HASTUR_AGENT_BIN,
        "--uuid", A1UUID,
        "--router", :router,
        "--unix",   :agent1unix,
        :stdout => :greenio, :stderr => :redio
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

