#!/usr/bin/env ruby

require "test/unit"
require 'hastur/message'
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
      :client           => Nodule::ZeroMQ.new(:connect => ZMQ::DEALER, :uri => :gen),
      :register_client  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :limit => 1),
      :notification     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :limit => 1),
      :heartbeat_client => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :limit => 1),
      :stat             => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :limit => 1),
      :log              => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :limit => 1),
      :error            => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :limit => 1),
      :plugin_exec      => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH,   :uri => :gen),
      :control          => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,    :uri => :gen),
      :acks             => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen),
      :rawdata          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen),
      :plugin_result    => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH,   :uri => :gen),
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

    # debug ... delete this after things are working
    @routes_to_test.each do |route|
      @topology[route].add_reader do |messages|
        STDERR.puts "[#{route}] saw message #{messages[-1]}"
      end
    end

    # run the tests inside handler blocks
    @routes_to_test.each do |route|
      @topology[route].add_reader do |messages|
        @count += 1

        e = Hastur::Envelope.parse(messages[-2])
        refute_nil e
        assert_equal route, e.route, "routed to #{route}"

        msg = Hastur::Message.new(:envelope => e, :payload => messages[-1])
        refute_nil msg
        assert_kind_of Hastur::Message, msg
      end
    end

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_routes
    socket = @topology[:client].socket

    @routes_to_test.each do |route|
      klass = Hastur::Message.route_class(route)
      msg = klass.new(:payload => MESSAGES[route], :from => C1UUID)
      # WTFBBQ - why is this not working?
      # failing with: 'Socket operation on non-socket'
      rc = msg.send(socket)
      assert rc > -1, "msg.send() must succeed to have a useful test"
    end

    @routes_to_test.each do |route|
      @topology[route].wait(3)
    end

    assert_equal 5, @count, "should have seen 5 messages"
  end
end
