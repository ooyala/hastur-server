#!/usr/bin/env ruby

require 'nodule/topology'
require 'nodule/process'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/console'

require 'multi_json'
require 'rainbow'

uuid1 = '11111111-2222-3333-4444-555555555555'
uuid2 = 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'

HASTUR_ROOT = File.join(File.dirname(__FILE__), "..")

topology = Nodule::Topology.new(
  :cyanio        => Nodule::Console.new(:fg => :cyan),
  :greenio       => Nodule::Console.new(:fg => :green),
  :yellowio      => Nodule::Console.new(:fg => :yellow),
  :redio         => Nodule::Console.new(:fg => :red),
  :router        => Nodule::ZeroMQ.new(:uri => :gen),
  :stat          => Nodule::ZeroMQ.new(:uri => :gen),
  :event         => Nodule::ZeroMQ.new(:uri => :gen),
  :heartbeat     => Nodule::ZeroMQ.new(:uri => :gen),
  :registration  => Nodule::ZeroMQ.new(:uri => :gen),
  :log           => Nodule::ZeroMQ.new(:uri => :gen),
  :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :redio),
  :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => :greenio),
  :direct        => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
  :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
  :routersvc     => Nodule::Process.new(
    File.join(HASTUR_ROOT, "bin", "hastur-router.rb"),
    '--uuid',          uuid1,
    '--hwm',           100,
    '--router',        :router,
    '--event',         :event,
    '--heartbeat',     :heartbeat,
    '--registration',  :registration,
    '--stat',          :stat,
    '--log',           :log,
    '--error',         :error,
    '--direct',        :direct,
    '--rawdata',       :rawdata,
    '--control',       :control,
    :stdout => :cyanio, :stderr => :cyanio, :verbose => :cyanio,
  ),
  :client1svc    => Nodule::Process.new(
    File.join(HASTUR_ROOT, "bin", "hastur-client.rb"),
    '--uuid',         uuid2,
    '--router',       :router,
    '--port',         8125,
    '--ack-timeout',  5,
    :stdout => :cyanio, :stderr => :cyanio, :verbose => :cyanio,
  ),
  :cass_sink => Nodule::Process.new(
    File.join(HASTUR_ROOT, "bin", "cass_sink.rb"),
    '--routers', :stat, :event, :heartbeat, :registration, :log,
    :verbose => :cyanio, :stderr => :redio, :stdout => :yellowio
  ),
)

topology.start_all

loop do
  sleep 30
end

