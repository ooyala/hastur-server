#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'termite'

require "hastur-server/message"
require "hastur-server/router"

Ecology.read("hastur-router.ecology")
MultiJson.engine = :yajl
logger = Termite::Logger.new

opts = Trollop::options do
  banner <<-EOS
hastur-router.rb - route to/from Hastur clients

  Options:
EOS
  opt :uuid,           "Router UUID (for logging)",      :type => String
  opt :hwm,            "ZeroMQ message queue depth",     :default => 1
  opt :router,         "Router (incoming) URI (ROUTER)", :default => "tcp://*:8126"
  opt :stat,           "Stat sink URI           (PUSH)", :default => "tcp://*:8127"
  opt :event,          "Event sink URI          (PUSH)", :default => "tcp://*:8128"
  opt :log,            "Log sink URI            (PUSH)", :default => "tcp://*:8129"
  opt :error,          "Error sink URI          (PUSH)", :default => "tcp://*:8130"
  opt :rawdata,        "Rawdata sink URI        (PUSH)", :default => "tcp://*:8131"
  opt :heartbeat,      "Heartbeat sink URI      (PUSH)", :default => "tcp://*:8132"
  opt :registration,   "Registration sink URI   (PUSH)", :default => "tcp://*:8133"
  opt :direct,         "Direct routing URI      (PULL)", :default => "tcp://*:8134"
  opt :control,        "Router control RPC URI   (REP)", :default => "tcp://127.0.0.1:8135"
end

ctx = ZMQ::Context.new

sockets = {
   :router         => ctx.socket(ZMQ::ROUTER),
   :stat           => ctx.socket(ZMQ::PUSH),
   :event          => ctx.socket(ZMQ::PUSH),
   :log            => ctx.socket(ZMQ::PUSH),
   :error          => ctx.socket(ZMQ::PUSH),
   :rawdata        => ctx.socket(ZMQ::PUSH),
   :heartbeat      => ctx.socket(ZMQ::PUSH),
   :registration   => ctx.socket(ZMQ::PUSH),
   :direct         => ctx.socket(ZMQ::PULL),
   :control        => ctx.socket(ZMQ::REP),
}

sockets.each do |key,sock|
  sock.setsockopt(ZMQ::LINGER, -1)
  sock.setsockopt(ZMQ::HWM, opts[:hwm])
  sock.setsockopt(ZMQ::IDENTITY, "#{opts[:uuid]}:#{key}")
  rc = sock.bind(opts[key])
  abort "Error binding #{key} socket: #{ZMQ::Util.error_string}" unless rc > -1
end

R = Hastur::Router.new(opts[:uuid], :error_socket => sockets[:error])

# set up signal handlers and hope to be able to get a clean shutdown
%w(INT TERM KILL).each do |sig|
  Signal.trap(sig) do
    R.shutdown
    Signal.trap(sig, "DEFAULT")
  end
end

# Client -> Sink static routes
R.route :to => :stat,              :src => sockets[:router], :dest => sockets[:stat],          :static => true
R.route :to => :event,             :src => sockets[:router], :dest => sockets[:event],         :static => true
R.route :to => :log,               :src => sockets[:router], :dest => sockets[:log],           :static => true
R.route :to => :error,             :src => sockets[:router], :dest => sockets[:error],         :static => true
R.route :to => :rawdata,           :src => sockets[:router], :dest => sockets[:rawdata],       :static => true
R.route :to => :heartbeat,         :src => sockets[:router], :dest => sockets[:heartbeat],     :static => true
R.route :to => :registration,      :src => sockets[:router], :dest => sockets[:registration],  :static => true

# (scheduler / acks) -> Clients static route
R.route :from => :direct, :src => sockets[:direct], :dest => sockets[:router], :static => true

R.handle sockets[:control] do |sock|
  begin
    rc = sock.recv_string json=""
    config = MultiJson.decode json, :symbolize_keys => true
    # TODO: route_del
    result = case config.delete(:method)
      when "shutdown";   R.shutdown; "Shutting down."
      when "route_add";  R.route config[:params]
      when "route_dump"; R.routes
      else
        raise ArgumentError.new "invalid command on control socket: #{json}"
    end
    sock.send_string MultiJson.encode({:result => result, :error => "", :id => config[:id]})
  rescue
  # TODO: log bad/unparsable commands
  end
end

# Run the router poll loop
R.run

# close all the sockets
sockets.each do |key,sock|
  sock.close
end

