#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"

require 'hastur/test/resource/unixsocket'
require 'hastur/test/resource/zeromq'
require 'hastur/test/resource/tty'
require 'hastur/test/process'
require 'hastur/test/topology'
require 'rainbow'
require 'multi_json'

class HeartbeatTest < Test::Unit::TestCase
  HTRZMQ = Hastur::Test::Resource::ZeroMQ
  def initialize(*args)
    @resources = {
      :greenio      => Hastur::Test::Resource::Tty.new(:fg => :green),
      :redio        => Hastur::Test::Resource::Tty.new(:fg => :red),
      :client1unix  => Hastur::Test::Resource::UnixSocket.new,
      :client2unix  => Hastur::Test::Resource::UnixSocket.new,
      :router       => HTRZMQ.new(:uri => :gen),
      :heartbeat    => HTRZMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :capture, :limit => 4),
      :register     => HTRZMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :notification => HTRZMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :stat         => HTRZMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :log          => HTRZMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :error        => HTRZMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :drain),
      :control      => HTRZMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
    }

    @processes = {
      :client1 => Hastur::Test::Process.new(@resources, {:stdout => :greenio, :stderr => :redio},
        HASTUR_CLIENT_BIN,
        '--uuid',      C1UUID,
        '--heartbeat', 1,
        '--router',    :router,
        '--unix',      :client1unix
      ),
      :client2 => Hastur::Test::Process.new(@resources, {:stdout => :greenio, :stderr => :redio},
        HASTUR_CLIENT_BIN,
        '--uuid',      C2UUID,
        '--heartbeat', 1,
        '--router',    :router,
        '--unix',      :client2unix
      ),
      :router => Hastur::Test::Process.new(@resources, {:stdout => :greenio, :stderr => :redio},
        HASTUR_ROUTER_BIN,
        '--uuid',         'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
        '--heartbeat',    :heartbeat,
        '--register',     :register,
        '--notification', :notify,
        '--stat',         :stat,
        '--log',          :log,
        '--error',        :error,
        '--router',       :router,
        '--plugin-exec',  :from_sink,
      ),
    }

    super(*args)
  end

  def setup
    @topology = Hastur::Test::Topology.new(@resources, @processes, :int_wait => 5)
    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_heartbeat
    # wait for some messages to flow
    sleep 3

    messages = @resources[:heartbeat].output
    # work with raw messages for now
    payloads  = messages.map { |m| MultiJson.decode(m[-1]) }
    envelopes = messages.map { |m| m[-2].unpack("H*") }
    #puts messages.flatten.map { |i| i.unpack("H*") }

    assert_equal 4, messages.count, "Should have exactly two captured messages"
    assert_kind_of Array, messages[0], "messages should be 2-level arrays"
    assert_kind_of Array, messages[1], "messages should be 2-level arrays"
    assert_kind_of Array, messages[2], "messages should be 2-level arrays"
    assert_kind_of Array, messages[3], "messages should be 2-level arrays"
    assert_equal 2, messages[0].count, "each message should have 2 parts"
    assert_equal 2, messages[1].count, "each message should have 2 parts"

    # verify that the messages on the heartbeat shims are heartbeat messages
    assert_equal(payloads.count, payloads.fuzzy_filter("heartbeat" => Fixnum).count)
    assert_equal(payloads.count, payloads.fuzzy_filter("last_heartbeat" => String).count)

    c1uuid = C1UUID.gsub('-', '')
    c2uuid = C2UUID.gsub('-', '')
    assert envelopes.flatten.any? { |e| e.include?(c1uuid) }, "No envelope contains client 1's UUID"
    assert envelopes.flatten.any? { |e| e.include?(c2uuid) }, "No envelope contains client 2's UUID"
  end
end

