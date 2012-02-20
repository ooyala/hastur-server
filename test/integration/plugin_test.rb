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
    plugin_text = MultiJson.encode(MultiJson.encode({
      :status  => 0,
      :message => "OK - plugin success!"
    }))
    @plugin_request = <<EOJSON
{
  "plugin_path": "/bin/echo",
  "plugin_args": #{plugin_text},
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
      :plugin_result => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture),
      :plugin_exec   => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen, :thread => false),
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
    puts "Going to wait for client to boot ..."
    @wait.synchronize { @rsrc.wait(@wait) }
    puts "Client sent registration, ready to go"

    rc = msg.send @topology[:plugin_exec].socket
    assert rc > -1, "zeromq send must return > -1"

    @wait.synchronize { @rsrc.wait(@wait) }

    envelope = nil
    message = nil
    assert_nothing_raised "Must be able to parse captured ZeroMQ messages" do
      messages = @topology[:plugin_result].output.pop
      envelope = Hastur::Envelope.parse messages[-2]
      message = Hastur::Message::PluginResult.new :envelope => envelope, :payload => messages[-1]
    end

    refute_nil envelope, "should have captured a valid envelope"
    refute_nil message, "should have captured a valid message"

    # TODO: fill in more tests after going back through Hastur::Message and adding standard parsing
    puts "PAYLOAD: #{message.payload}"

    #assert 4 <= @ack_proc_calls, "The ack receiver proc should be called at least 4 times (got #{@ack_proc_calls})."
    # verify that the messages on the heartbeat shims are heartbeat messages
    #assert_equal(messages.size, messages.fuzzy_filter( {"method" => "stats"} ).size)
    # verify that the count of messages on the heartbeat shims are accurate
    #assert_equal(1, messages.size)
  end
end
