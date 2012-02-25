#!/usr/bin/env ruby

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "..", "lib")

require "test/unit"
require 'hastur-server/message'
require 'nodule/topology'
require 'nodule/process'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/console'
require_relative "./integration_test_helper"

class NotificationTest < Test::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :greenio          => Nodule::Console.new(:fg => :green),
      :redio            => Nodule::Console.new(:fg => :red),
      :cyanio           => Nodule::Console.new(:fg => :cyan),
      :client           => Nodule::ZeroMQ.new(:connect => ZMQ::DEALER, :uri => :gen, :reader => :drain),
      :register_client  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :notification     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :heartbeat_client => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :stat             => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :log              => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :error            => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :rawdata          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :drain),
      :plugin_result    => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :drain),
      :plugin_exec      => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH,   :uri => :gen, :thread => false),
      :acks             => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH,   :uri => :gen, :thread => false),
      :control          => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,    :uri => :gen, :thread => false),
      :routersvc        => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :client,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--rawdata',       :rawdata,
        '--acks',          :acks,
        '--heartbeat',     :heartbeat_client,
        '--register',      :register_client,
        '--notification',  :notification,
        '--plugin-exec',   :plugin_exec,
        '--plugin-result', :plugin_result,
        '--control',       :control,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    @count = 0

    # test a selection of routes, injecting a message on the ROUTER socket, then check
    # the receiver sockets to make sure they got routed and routed to the right socket
    @routes_to_test = [:heartbeat_client, :register_client, :notification, :stat, :log, :error]

    # run the tests inside handler blocks
    @routes_to_test.each do |route|
      @topology[route].add_reader do |messages|
        @count += 1

        e = Hastur::Envelope.parse(messages[-2])
        puts  "Got #{e.to}"
        refute_nil e
        rid = Hastur.route_id(route)
        assert_equal rid, e.to, "routed to #{route}"
      end
    end

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_routes
    @routes_to_test.each do |route|
      klass = Hastur::Message.route_class(route)
      msg = klass.new(:payload => MESSAGES[route], :from => C1UUID)
      rc = msg.send @topology[:client].socket
      assert rc > -1, "msg.send() must succeed to have a useful test"
    end

    @routes_to_test.each do |route|
      @topology[route].wait(1)
      STDERR.puts "GOT: #{@topology[route].output}"
    end

    assert_equal 6, @count, "should have seen 6 messages"
  end
end
