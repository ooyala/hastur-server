#!/usr/bin/env ruby

require_relative "./integration_test_helper"
require 'minitest/autorun'
require 'hastur-server/rpc/client'
require 'hastur-server/rpc/server'
require 'nodule'
require 'nodule/zeromq'

class TestClassHasturRPC < MiniTest::Unit::TestCase
  def setup
    Thread.abort_on_exception = true

    @topology = Nodule::Topology.new(
      :greenio => Nodule::Console.new(:fg => :green),
      :redio   => Nodule::Console.new(:fg => :red),
      :server  => Nodule::ZeroMQ.new(:uri => :gen),
    )
    @topology.start_all

    server_thread = Thread.new do
      server = Hastur::RPC::Server.new @topology[:server].uri

      server.add_handler :puts do |data|
        { :item => 10 }
      end

      server.run
    end

    Thread.pass
  end

  def test_rpc_client_server
    client = Hastur::RPC::Client.new @topology[:server].uri
    refute_nil client

    response = client.request :puts, {:abc => 2}
    assert_equal 10, response[:item], "data in the right format"
  end
end
