#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'
require 'hastur-server/message'

class NotificationTest < Test::Unit::TestCase
  ITERATIONS = 4

  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :client1unix   => Nodule::UnixSocket.new,
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => ITERATIONS),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :cyanio),
      :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :routersvc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--hwm',           10000,
        '--router',        :router,
        '--event',         :event,
        '--heartbeat',     :heartbeat,
        '--registration',  :registration,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--direct',        :direct,
        '--rawdata',       :rawdata,
        '--control',       :control,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :client1svc    => Nodule::Process.new(
        HASTUR_CLIENT_BIN,
        '--uuid',         C1UUID,
        '--router',       :router,
        '--unix',         :client1unix,
        '--ack-timeout',  1,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
    )

    @events_seen = 0

    @topology[:event].add_reader proc { |messages|
      e = Hastur::Envelope.parse(messages[-2])
      assert_not_nil e, "Hastur::Envelope.parse on messages[-2] must return an envelope."
      assert e.ack?, "Events must always have the ack flag enabled (got: #{e.ack})."
      @events_seen += 1

      # send an ack, since it's the right thing to do
      rc = e.to_ack.send @topology[:direct].socket
      assert rc > -1, "sending an ack created from the envelope of the message"
    }

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_event
    # send an event
    event = <<EOJSON
{
  "_route": "event",
  "sla": 604800,
  "app": "dyson",
  "recipients": [
    "backlot-oncall",
    "backlot-fyi",
    "backlot-operations"
  ]
}
EOJSON

    @topology[:heartbeat].require_read_count 1

    ITERATIONS.times do
      @topology[:client1unix].send event
    end

    @topology[:event].require_read_count ITERATIONS, 3 do
      flunk "timeout waiting for #{ITERATIONS} events (had #{@topology[:event].read_count})"
    end

    messages = @topology[:event].output
    payloads = messages.map { |m| MultiJson.decode(m[-1]) }

    assert_equal ITERATIONS, payloads.size
    assert_equal 604800, payloads[0]["sla"]
    assert_equal ITERATIONS, messages.size

    assert ITERATIONS <= @events_seen, "The ack receiver proc should be called at least #{ITERATIONS} times (got #{@events_seen})."
  end
end
