#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'ffi-rzmq'
require 'securerandom'
require 'socket'

require 'hastur/client'
require 'hastur/router'
require 'hastur/message'

class TestClassHasturRouterModule < MiniTest::Unit::TestCase
  ROUTER_UUID = SecureRandom.uuid
  CLIENT_UUID = SecureRandom.uuid
  SCHED_UUID  = SecureRandom.uuid

  ROUTER_URI  = 'ipc://router'
  ROUTER_URIS = {
    :client_router     => [ZMQ::ROUTER, ROUTER_URI],
    :stat              => [ZMQ::PUSH,   'ipc://stat-sink'],
    :log               => [ZMQ::PUSH,   'ipc://log-sink'],
    :error             => [ZMQ::PUSH,   'ipc://error-sink'],
    :rawdata           => [ZMQ::PUSH,   'ipc://rawdata-sink'],
    :notification      => [ZMQ::PUSH,   'ipc://notification-sink'],
    :heartbeat         => [ZMQ::PUSH,   'ipc://heartbeat-sink'],
    :plugin_result     => [ZMQ::PUSH,   'ipc://plugin-sink'],
    :plugin_exec       => [ZMQ::PULL,   'ipc://plugin-exec'],
    :register          => [ZMQ::PUSH,   'ipc://registration-sink'],
  }

  def initialize(*args)
    @sink_running = true
    @router_running = true
    @ctx = ZMQ::Context.new
    @mutex = Mutex.new
    @sink_messages = 0
    super(*args)
  end

  def zmq_sockopts(s)
    s.setsockopt(ZMQ::LINGER, -1)
    if ZMQ::LibZMQ.version2?
      s.setsockopt(ZMQ::HWM, 1)
    elsif  ZMQ::LibZMQ.version3?
      s.setsockopt(ZMQ::SNDHWM, 1)
      s.setsockopt(ZMQ::RCVHWM, 1)
    end
  end

  def client_stub
    client = @ctx.socket(ZMQ::DEALER) 

    zmq_sockopts(client)
    client.connect ROUTER_URIS[:client_router][1]

    reg = Hastur::Message::RegisterClient.new(
      :from => CLIENT_UUID,
      :payload => "oh, hai!"
    )

    assert reg.send(client) != -1
    @mutex.synchronize { @sink_messages += 1 }

    pexec = Hastur::Message.recv(client)
    assert_kind_of Hastur::Message::PluginExec, pexec

    assert reg.send(client) != -1
    @mutex.synchronize { @sink_messages += 1 }

    # messages received on the client should not have any zmq envelopes
    assert pexec.zmq_parts.empty?, "Received client message should have no ZMQ envelope parts."
    assert reg.zmq_parts.empty?, "Generated registration message should have no ZMQ envelope parts."

    client.close
  end

  def run_router
    router_sockets = {}

    ROUTER_URIS.each do |key, opts|
      socket = @ctx.socket(opts[0])
      zmq_sockopts(socket)
      socket.bind(opts[1])
      router_sockets[key] = socket
    end

    # create a router instance
    router = Hastur::Router.new(ROUTER_UUID)

    client_router = router_sockets.delete :client_router

    Hastur::ROUTES.keys.each do |route|
      dest = router_sockets[route]

      # heartbeats / registrations are merged onto two sinks (5 -> 2)
      if route.to_s =~ /^heart/
        dest = router_sockets[:heartbeat]
      elsif route.to_s =~ /^register/
        dest = router_sockets[:register]
      elsif route == :plugin_exec
        router.route :from => :plugin_exec, :src => dest, :dest => client_router, :static => true
      elsif not router_sockets.has_key? route
        next
      end

      # set up a routing rule
      router.route :to => route, :src => client_router, :dest => dest, :static => true
    end

    count = 0
    while @router_running
      router.poll(1)
      break if count > 20
    end

    router.free_dynamic
    router_sockets.each { |key,sock| sock.close }

    client_router.close
  end

  def sink_stub
    puller = @ctx.socket(ZMQ::PULL)
    zmq_sockopts(puller)

    # pretend to be all of the sinks
    ROUTER_URIS.each do |key, opts|
      puller.connect(opts[1]) if opts[0] == ZMQ::PUSH
    end

    count = 0
    while @sink_running
      if @mutex.synchronize { @sink_messages <= count }
        sleep 0.2
      else
        msg = Hastur::Message.recv(puller)

        # endpoints should not have to deal with router parts, make sure they've been stipped
        assert msg.zmq_parts.empty?, "Received sink message should have no ZMQ envelope parts."
        # of course, if this assert fails, there's a decent chance of segfaulting

        count += 1
      end
    end

    puller.close
  end

  def scheduled_stub
    scheduler = @ctx.socket(ZMQ::PUSH) 
    zmq_sockopts(scheduler)
    scheduler.connect ROUTER_URIS[:plugin_exec][1]

    plugin = Hastur::Message::PluginExec.new(
      :to   => CLIENT_UUID,
      :data => {
        :_route      => :plugin_exec,
        :plugin_path => "/bin/echo",
        :plugin_args => "random.1"
      }
    )
    
    assert plugin.send(scheduler) != -1 # schedule the plugin

    scheduler.close
  end

  def test_router
    # run the router in a thread
    router_thread = Thread.new { run_router rescue STDERR.puts $!.inspect, $@ }

    # pretend to be all of the sinks in a thread
    sink_thread = Thread.new { sink_stub rescue STDERR.puts $!.inspect, $@ }

    # pretend to be a client
    client_thread = Thread.new { client_stub rescue STDERR.puts $!.inspect, $@ }

    # pretend to be a scheduled
    scheduled_thread = Thread.new { scheduled_stub rescue STDERR.puts $!.inspect, $@ }

    scheduled_thread.join

    client_thread.join

    @sink_running = false
    sink_thread.join

    @router_running = false
    router_thread.join

    @ctx.terminate
  end
end

