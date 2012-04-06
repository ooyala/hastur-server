#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'multi_json'
require 'hastur-server/message'
require 'hastur-server/mock/nodule_router'

class EventTest < Test::Unit::TestCase
  ITERATIONS = 4

  def setup
    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :agent1unix    => Nodule::UnixSocket.new,
      #:router        => Nodule::ZeroMQ.new(:uri => :gen, :bind => ZMQ::ROUTER, :reader => :capture),
      :router        => Hastur::Mock::NoduleRouter.new,
      :agent1svc     => Nodule::Process.new(
        HASTUR_AGENT_BIN,
        '--uuid',         C1UUID,
        '--router',       :router,
        '--unix',         :agent1unix,
        '--ack-timeout',  1,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
    )

    @events_seen = 0

    @topology[:router].add_reader proc { |messages|
      e = Hastur::Envelope.parse(messages[-2])

      if e.type_symbol == :event
        assert e.ack?, "Events must always have the ack flag enabled (got: #{e.ack})."
        @events_seen += 1

        # send an ack, since it's the right thing to do
        rc = e.to_ack.send @topology[:router].socket
        assert rc > -1, "sending an ack created from the envelope of the message"
      elsif e.type_symbol == :error
        flunk "Hastur::Message::Error: #{messages.inspect}"
      end
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
  "type": "event",
  "sla": 604800,
  "app": "dyson",
  "recipients": [
    "backlot-oncall",
    "backlot-fyi",
    "backlot-operations"
  ]
}
EOJSON
    # The agent will send three messages at startup, a heartbeat and a noop, and a registration
    @topology[:router].require_read_count 3, 30
    @topology[:router].clear!

    ITERATIONS.times do
      @topology[:agent1unix].send event
      sleep 0.1
    end

    @topology[:router].require_read_count ITERATIONS do
      flunk "timeout waiting for #{ITERATIONS} events (had #{@topology[:router].read_count})"
    end

    messages = @topology[:router].output
    payloads = messages.map { |m| MultiJson.decode(m[-1]) }

    assert_equal ITERATIONS, payloads.size
    assert_equal 604800, payloads[0]["sla"]
    assert_equal ITERATIONS, messages.size

    assert ITERATIONS <= @events_seen, "The ack receiver proc should be called at least #{ITERATIONS} times (got #{@events_seen})."
  end
end
