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
    @ack_proc_calls = 0

    ack = proc do |messages|
      STDERR.puts "Notification!"
      e = Hastur::Envelope.parse(messages[-2])
      refute_nil e, "Hastur::Envelope.parse on messages[-2] must return an envelope."
      assert e.ack?, "Notifications must always have the ack flag enabled (got: #{e.ack})."
      @ack_proc_calls += 1
    end

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :client1unix   => Nodule::UnixSocket.new,
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :notification  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => ack, :limit => 4),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :plugin_result => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :plugin_exec   => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :acks          => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :routersvc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :router,
        '--notification',  :notification,
        '--heartbeat',     :heartbeat,
        '--registration',  :registration,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--plugin-exec',   :plugin_exec,
        '--plugin-result', :plugin_result,
        '--acks',          :acks,
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

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_notification
    # send a notification
    notification = <<EOJSON
{
  "_route": "notification",
  "sla": 604800,
  "app": "dyson",
  "recipients": [
    "backlot-oncall",
    "backlot-fyi",
    "backlot-operations"
  ]
}
EOJSON

    sleep 1
    puts "Sending notifications ..."

    @topology[:client1unix].send notification
    @topology[:client1unix].send notification
    @topology[:client1unix].send notification
    @topology[:client1unix].send notification

    messages = @topology[:notification].output
    payloads = messages.map { |m| MultiJson.decode(m[-1]) }
    assert_equal 4, payloads.size
    assert_equal 604800, payloads[0]["sla"]
    assert_equal 4, messages.size
    @topology[:notification].wait(5)

    assert 4 <= @ack_proc_calls, "The ack receiver proc should be called at least 4 times (got #{@ack_proc_calls})."
  end
end
