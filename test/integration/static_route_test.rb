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
    set_test_alarm(30)

    @topology = Nodule::Topology.new(
      :greenio          => Nodule::Console.new(:fg => :green),
      :redio            => Nodule::Console.new(:fg => :red),
      :cyanio           => Nodule::Console.new(:fg => :cyan),
      :client           => Nodule::ZeroMQ.new(:connect => ZMQ::DEALER, :uri => :gen),
      :registration     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :event            => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :heartbeat        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :stat             => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :log              => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :error            => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :rawdata          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :drain),
      :direct           => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH,   :uri => :gen),
      :control          => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,    :uri => :gen),
      :routersvc        => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :client,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--rawdata',       :rawdata,
        '--heartbeat',     :heartbeat,
        '--registration',  :registration,
        '--event',         :event,
        '--direct',        :direct,
        '--control',       :control,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    # test a selection of routes, injecting a message on the ROUTER socket, then check
    # the receiver sockets to make sure they got routed and routed to the right socket
    @routes_to_test = [:error, :heartbeat, :registration, :event, :stat, :log]
    @message_count = @routes_to_test.count

    # run the tests inside handler blocks
    @count = 0
    @routes_to_test.each do |route|
      @topology[route].add_reader do |messages|
        @count += 1 
        e = Hastur::Envelope.parse(messages[-2])
        refute_nil e
        klass = Hastur::Message.symbol_to_class(route.to_sym)
        rid = klass.route_uuid
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
      klass = Hastur::Message.symbol_to_class(route.to_sym)
      msg = klass.new(:payload => "{}", :from => C1UUID)
      rc = msg.send @topology[:client].socket
      assert rc > -1, "msg.send() must succeed to have a useful test"
    end

    messages = {}
    @routes_to_test.each do |route|
      @topology.wait route, 1
      messages[route] = @topology[route].output
    end

    assert_equal @message_count, @count, "should have seen #{@message_count} messages"
  end
end
