#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require "test/unit"
require 'nodule'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'hastur-server/message'

class StaticRouteTest < Test::Unit::TestCase
  def setup
    @topology = Nodule::Topology.new(
      :alarm         => Nodule::Alarm.new(:timeout => 30),
      :greenio          => Nodule::Console.new(:fg => :green),
      :redio            => Nodule::Console.new(:fg => :red),
      :cyanio           => Nodule::Console.new(:fg => :cyan),
      :agent            => Nodule::ZeroMQ.new(:connect => ZMQ::DEALER, :uri => :gen),
      :registration     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :event            => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :heartbeat        => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :stat             => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :log              => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :error            => Nodule::ZeroMQ.new(:connect => ZMQ::PULL,   :uri => :gen, :reader => :capture, :limit => 1),
      :direct           => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH,   :uri => :gen),
      :control          => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,    :uri => :gen),
      :routersvc        => Nodule::Process.new(
        HASTUR_ROUTER_BIN,
        '--uuid',          R1UUID,
        '--router',        :agent,
        '--stat',          :stat,
        '--log',           :log,
        '--error',         :error,
        '--heartbeat',     :heartbeat,
        '--registration',  :registration,
        '--event',         :event,
        '--direct',        :direct,
        '--control',       :control,
        :stdout => :greenio, :stderr => :redio, :verbose => :cyanio
      ),
    )

    # test a selection of types, injecting a message on the ROUTER socket, then check
    # the receiver sockets to make sure they got routed and routed to the right socket
    @types_to_test = {
      :error => :error,
      :hb_process => :heartbeat,
      :reg_agent => :registration,
      :event => :event,
      :counter => :stat,
      :log => :log
    }
    @message_count = @types_to_test.size

    # run the tests inside handler blocks
    @count = 0
    # the symbols used in the topology setup above must match the hastur type symbols for this to work
    @types_to_test.each do |type_symbol, socket_symbol|
      klass = Hastur::Message.symbol_to_class(type_symbol)
      @topology[socket_symbol].add_reader do |messages|
        @count += 1 
        e = Hastur::Envelope.parse(messages[-2])
        assert_not_nil e

        assert_equal e.type_id, klass.type_id

        msg = klass.new :envelope => e, :payload => messages[-1]
        assert_not_nil msg
      end
    end

    @topology.start_all
  end

  def teardown
    @topology.stop_all
  end

  def test_routes
    @types_to_test.each do |type_symbol, _|
      klass = Hastur::Message.symbol_to_class(type_symbol)
      msg = klass.new(:payload => "{}", :from => A1UUID)
      rc = msg.send @topology[:agent].socket
      assert rc > -1, "msg.send() must succeed to have a useful test"
    end

    messages = {}
    @types_to_test.each do |type_symbol, socket_symbol|
      @topology.wait socket_symbol, 1
      messages[type_symbol] = @topology[socket_symbol].output
    end

    assert_equal @message_count, @count, "should have seen #{@message_count} messages"
  end
end
