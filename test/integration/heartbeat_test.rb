#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"

require 'hastur/test/resource/unixsocket'
require 'hastur/test/resource/zeromq'
require 'hastur/test/process'
require 'hastur/test/topology'
require 'rainbow'

class HeartbeatTest < Test::Unit::TestCase

  HTRZMQ = Hastur::Test::Resource::ZeroMQ
  def initialize(*args)
    String.send(:include, Sickill::Rainbow)

    @resources = {
      :client1      => Hastur::Test::Resource::UnixSocket.new,
      :client2      => Hastur::Test::Resource::UnixSocket.new,
      :greenio      => Hastur::Test::Resource::Base.new(:action => proc {|line| puts line.color(:green)}),
      :redio        => Hastur::Test::Resource::Base.new(:action => proc {|line| puts line.color(:red)}),
      :router       => HTRZMQ.new(:type => ZMQ::ROUTER, :bind => :gen),
      :heartbeat    => HTRZMQ.new(:type => ZMQ::PUSH,   :bind => :gen, :action => :capture),
      :register     => HTRZMQ.new(:type => ZMQ::PUSH,   :bind => :gen, :action => :drain),
      :notification => HTRZMQ.new(:type => ZMQ::PUSH,   :bind => :gen, :action => :drain),
      :stat         => HTRZMQ.new(:type => ZMQ::PUSH,   :bind => :gen, :action => :drain),
      :log          => HTRZMQ.new(:type => ZMQ::PUSH,   :bind => :gen, :action => :drain),
      :error        => HTRZMQ.new(:type => ZMQ::PUSH,   :bind => :gen, :action => :drain),
      :control      => HTRZMQ.new(:type => ZMQ::REP,    :bind => :gen),
    }
  
    @processes = {
      :client1 => Hastur::Test::Process.new(@resources, {:stdout => :greenio, :stderr => :redio},
        HASTUR_CLIENT_BIN, '--heartbeat', 5, '--router', :router, '--unix', :client1
      ),
      :client2 => Hastur::Test::Process.new(@resources, {:stdout => :greenio, :stderr => :redio},
        HASTUR_CLIENT_BIN, '--heartbeat', 5, '--router', :router, '--unix', :client2
      ),
      :router => Hastur::Test::Process.new(@resources, {:stdout => :greenio, :stderr => :redio},
        HASTUR_ROUTER_BIN,
        '--uuid',         '9eb8fe2b-31fe-4779-ae43-d7d0ddf23ebb',
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
    puts "Starting up all of the topology components..."
    @topology.start_all
    puts "Started up all of the topology components..."
  end

  def teardown
    puts "Tearing down all of the topology components..."
    @topology.stop_all
    puts "Topology is torn down..."
  end

  def test_heartbeat
    puts "Sleeping for 2 seconds..."
    sleep 2
    # get messages from the sink shims
    puts "Retrieving packets from heartbeat..."
    #messages = Hastur::Test::ZMQ.all_payloads_to(:heartbeat)
    # verify that the messages on the heartbeat shims are heartbeat messages
    #assert_equal(messages.size, messages.fuzzy_filter( {"method" => "heartbeat"} ).size)
    ## verify that the count of messages on the heartbeat shims are accurate
    #assert_equal(2, messages.size)
    puts "Exiting"
  end
end

