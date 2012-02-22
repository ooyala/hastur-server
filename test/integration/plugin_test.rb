#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"
require 'hastur/message'
require 'multi_json'
require 'nodule/topology'
require 'nodule/process'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/console'

class PluginTest < Test::Unit::TestCase
  def setup
    @plugin_text = MultiJson.encode("{\"status\": 0, \"message\": \"OK - plugin success!\"}")
    @plugin_request = <<EOJSON
{
  "plugin_path": "/bin/echo",
  "plugin_args": [#{@plugin_text}],
  "timestamp": #{Time.now.to_f * 1_000_000}
}
EOJSON

    @wait = Mutex.new
    @rsrc = ConditionVariable.new

    ready = proc { @wait.synchronize { @rsrc.signal } }

    @topology = Nodule::Topology.new(
      :greenio       => Nodule::Console.new(:fg => :green),
      :redio         => Nodule::Console.new(:fg => :red),
      :cyanio        => Nodule::Console.new(:fg => :cyan),
      :client1unix   => Nodule::UnixSocket.new,
      :router        => Nodule::ZeroMQ.new(:uri => :gen),
      :notification  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :register      => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => ready),
      :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
      :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :stderr),
      :plugin_result => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => 1),
      :plugin_exec   => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :acks          => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
      :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
      :routersvc     => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :router,
        '--notification',  :notification,
        '--heartbeat',     :heartbeat,
        '--register',      :register,
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

    # block again waiting for a message on plugin_result
    @topology[:plugin_result].add_reader ready

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_plugin
    msg = Hastur::Message::PluginExec.new(:from => R2UUID, :to => C1UUID, :payload => @plugin_request)

    # This should probably be a built-in for Nodule.
    @wait.synchronize { @rsrc.wait(@wait) }

    rc = msg.send @topology[:plugin_exec].socket
    assert rc > -1, "zeromq send must return > -1 (errno: #{ZMQ::Util.error_string})"


    @topology[:plugin_result].wait

    messages = @topology[:plugin_result].output.pop
    refute_nil messages, "should have caught some zmq messages"
    assert messages.count > 0, "should have caught some zmq messages"
    envelope = Hastur::Envelope.parse messages[-2]
    refute_nil envelope, "should have captured a valid envelope"
    message = Hastur::Message::PluginResult.new :envelope => envelope, :payload => messages[-1]
    refute_nil message, "should have captured a valid message"

    data = message.decode
    assert_kind_of Hash, data, "message.decode must return a hash"

    assert_kind_of Fixnum, data[:pid], "plugin result 'pid' should be a number"
    assert_kind_of Fixnum, data[:exit], "plugin result 'exit' should be a number"
    assert_equal 0, data[:exit], "plugin result should be 0"

    # TODO: client plugin output isn't parsed - this is a bug that must be fixed first
  end
end
