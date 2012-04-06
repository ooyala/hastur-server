#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'multi_json'
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'hastur-server/message'

class PluginTest < Test::Unit::TestCase
  def setup
    set_test_alarm(30) # helper
    @plugin_text = MultiJson.encode("{\"status\": 0, \"message\": \"OK - plugin success!\"}")
    @plugin_request = <<EOJSON
{
  "plugin_path": "/bin/echo",
  "plugin_args": [#{@plugin_text}],
  "timestamp": #{Time.now.to_f * 1_000_000}
}
EOJSON

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :event         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture),
      :registration  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain,   :limit => 1),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
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
        '--control',       :control,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
      :agent1svc     => Nodule::Process.new(
        HASTUR_AGENT_BIN,
        '--uuid',         A1UUID,
        '--router',       :router,
        '--ack-timeout',  1,
        '--heartbeat',    300,
        '--port',         HASTUR_UDP_PORT,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio,
      ),
    )

    @topology.start_all
  end

  def teardown
    set_test_alarm(3) # helper
    @topology.stop_all
    cancel_test_alarm
  end

  def test_plugin
    msg = Hastur::Message::Cmd::PluginV1.new(:to => A1UUID, :from => A2UUID, :payload => @plugin_request)

    @topology[:registration].require_read_count 1, 10

    rc = msg.send @topology[:direct].socket
    assert rc > -1, "zeromq send must return > -1 (errno: #{ZMQ::Util.error_string})"

    @topology[:heartbeat].require_read_count 2, 10

    messages = @topology[:heartbeat].output.pop
    assert_not_nil messages, "should have caught some zmq messages"
    assert messages.count > 0, "should have caught some zmq messages"
    envelope = Hastur::Envelope.parse messages[-2]
    assert_not_nil envelope, "should have captured a valid envelope"

    message = Hastur::Message::HB::Agent.new :envelope => envelope, :payload => messages[-1]
    assert_not_nil message, "should have captured a valid message"

    data = message.decode
    assert_kind_of Hash, data, "message.decode must return a hash"
    plugin_info = data[:labels]
    assert_kind_of Hash, plugin_info, ":labels value must return a hash"

    assert_kind_of Fixnum, plugin_info[:pid], "plugin result 'pid' should be a number"
    assert_kind_of Fixnum, plugin_info[:exit], "plugin result 'exit' should be a number"
    assert_equal 0, plugin_info[:exit], "plugin result should be 0"

    # TODO: agent plugin output isn't parsed - this is a bug that must be fixed first
  end
end
