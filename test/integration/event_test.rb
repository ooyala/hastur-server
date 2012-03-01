#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"
require 'hastur-server/message'
require 'multi_json'
require 'nodule/topology'
require 'nodule/process'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/console'

class NotificationTest < Test::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :client1unix   => Nodule::UnixSocket.new,
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => 4),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain,   :limit => 1),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
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

    @topology[:event].add_reader do |messages|
      e = Hastur::Envelope.parse(messages[-2])
      refute_nil e, "Hastur::Envelope.parse on messages[-2] must return an envelope."
      assert e.ack?, "Events must always have the ack flag enabled (got: #{e.ack})."
      @events_seen += 1
      STDERR.puts "Received event in test proc! (#{@events_seen})"
    end

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

    @topology[:heartbeat].wait 1

    @topology[:client1unix].send event
    @topology[:client1unix].send event
    @topology[:client1unix].send event
    @topology[:client1unix].send event

    @topology[:event].wait 10

    messages = @topology[:event].output
    payloads = messages.map { |m| MultiJson.decode(m[-1]) }

    assert_equal 4, payloads.size
    assert_equal 604800, payloads[0]["sla"]
    assert_equal 4, messages.size

    assert 4 <= @events_seen, "The ack receiver proc should be called at least 4 times (got #{@events_seen})."
  end
end
