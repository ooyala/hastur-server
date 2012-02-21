#!/usr/bin/env ruby

require 'nodule/topology'
require 'nodule/process'
require 'nodule/unixsocket'
require 'nodule/zeromq'
require 'nodule/console'

require 'hastur/message'
require 'multi_json'
require 'rainbow'

uuid1 = '11111111-2222-3333-4444-555555555555'
uuid2 = 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'

hastur_msg = proc do |messages|
  begin
    m = Hastur::Message.parse(messages[-2], messages[-1])

    case m
      when Hastur::Message::Stat
        stat = m.decode
        value = stat[:value] or stat[:increment]
        puts "#{stat[:name].to_s.ljust(70)} #{stat[:type].ljust(8)} #{value.to_s.ljust(20)} #{stat[:timestamp]}".color(:blue)

      # TODO: print out other message types
      when Hastur::Message::Log
      when Hastur::Message::Error
      when Hastur::Message::Rawdata
      when Hastur::Message::Notification
      when Hastur::Message::HeartbeatClient
      when Hastur::Message::HeartbeatService
      when Hastur::Message::PluginExec
      when Hastur::Message::PluginResult
      when Hastur::Message::RegisterClient
      when Hastur::Message::RegisterPlugin
      when Hastur::Message::RegisterService
      else
        raise "Unrecognized message type!?!?"
    end
  rescue Exception => e
    STDERR.puts "Error parsing Hastur message: #{e}".color(:red)
  end
end

topology = Nodule::Topology.new(
  :cyanio        => Nodule::Console.new(:fg => :cyan),
  :router        => Nodule::ZeroMQ.new(:uri => :gen),
  :notification  => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :heartbeat     => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :register      => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :stat          => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :log           => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :error         => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :rawdata       => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :plugin_result => Nodule::ZeroMQ.new(:connect => ZMQ::PULL, :uri => :gen, :reader => hastur_msg),
  :plugin_exec   => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
  :acks          => Nodule::ZeroMQ.new(:connect => ZMQ::PUSH, :uri => :gen),
  :control       => Nodule::ZeroMQ.new(:connect => ZMQ::REQ,  :uri => :gen),
  :routersvc     => Nodule::Process.new(
    '../infrastructure/hastur-router.rb',
    '--uuid',          uuid1,
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
    :stdout => :cyanio, :stderr => :cyanio, :verbose => :cyanio,
  ),
  :client1svc    => Nodule::Process.new(
    '../bin/hastur-client.rb',
    '--uuid',         uuid2,
    '--router',       :router,
    '--port',         8125,
    '--ack-timeout',  5,
    :stdout => :cyanio, :stderr => :cyanio, :verbose => :cyanio,
  ),
)

topology.start_all

loop do
  sleep 30
end

