#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'termite'

require "hastur/message"
require "hastur/router"

Ecology.read("hastur-router.ecology")
MultiJson.engine = :yajl
logger = Termite::Logger.new

opts = Trollop::options do
  banner <<-EOS
hastur-router.rb - route to/from Hastur clients

  Options:
EOS
  opt :uuid,           "Router UUID (for logging)",      :required => true, :type => String
  opt :router,         "Router (incoming) URI (ROUTER)", :default => "tcp://*:8126"
  opt :stat,           "Stat sink URI           (PUSH)", :default => "tcp://*:8127"
  opt :log,            "Log sink URI            (PUSH)", :default => "tcp://*:8128"
  opt :acks,           "Ack delivery            (PULL)", :default => "tcp://*:8128"
  opt :error,          "Error sink URI          (PUSH)", :default => "tcp://*:8130"
  opt :rawdata,        "Rawdata sink URI        (PUSH)", :default => "tcp://*:8131"
  opt :notification,   "Notification sink URI   (PUSH)", :default => "tcp://*:8132"
  opt :heartbeat,      "Heartbeat sink URI      (PUSH)", :default => "tcp://*:8133"
  opt :plugin_result,  "Plugin result sink URI  (PUSH)", :default => "tcp://*:8134"
  opt :plugin_exec,    "Plugin exec URI         (PULL)", :default => "tcp://*:8135"
  opt :register,       "Registration sink URI   (PUSH)", :default => "tcp://*:8136"
  opt :control,        "Router control RPC URI   (REP)", :default => "tcp://127.0.0.1:8137"
end

ctx = ZMQ::Context.new

sockets = {
   :router         => ctx.socket(ZMQ::ROUTER),
   :stat           => ctx.socket(ZMQ::PUSH),
   :log            => ctx.socket(ZMQ::PUSH),
   :acks           => ctx.socket(ZMQ::PULL),
   :error          => ctx.socket(ZMQ::PUSH),
   :rawdata        => ctx.socket(ZMQ::PUSH),
   :notification   => ctx.socket(ZMQ::PUSH),
   :heartbeat      => ctx.socket(ZMQ::PUSH),
   :plugin_result  => ctx.socket(ZMQ::PUSH),
   :plugin_exec    => ctx.socket(ZMQ::PULL),
   :register       => ctx.socket(ZMQ::PUSH),
   :control        => ctx.socket(ZMQ::REP),
}

sockets.each do |key,sock|
  sock.setsockopt(ZMQ::LINGER, -1)
  sock.setsockopt(ZMQ::HWM,     1)
  rc = sock.bind(opts[key])
  abort "Error binding #{key} socket: #{ZMQ::Util.error_string}" unless rc > -1
end

R = Hastur::Router.new(opts[:uuid])

# set up signal handlers and hope to be able to get a clean shutdown
%w(INT TERM KILL).each do |sig|
  Signal.trap(sig) do
    R.shutdown
    Signal.trap(sig, "DEFAULT")
  end
end

# Client -> Sink static routes
R.route :to => :stat,              :src => sockets[:router], :dest => sockets[:stat],          :static => true
R.route :to => :log,               :src => sockets[:router], :dest => sockets[:log],           :static => true
R.route :to => :error,             :src => sockets[:router], :dest => sockets[:error],         :static => true
R.route :to => :rawdata,           :src => sockets[:router], :dest => sockets[:rawdata],       :static => true
R.route :to => :notification,      :src => sockets[:router], :dest => sockets[:notification],  :static => true
R.route :to => :heartbeat_client,  :src => sockets[:router], :dest => sockets[:heartbeat],     :static => true
R.route :to => :heartbeat_service, :src => sockets[:router], :dest => sockets[:heartbeat],     :static => true
R.route :to => :plugin_result,     :src => sockets[:router], :dest => sockets[:plugin_result], :static => true
R.route :to => :register_client,   :src => sockets[:router], :dest => sockets[:register],      :static => true
R.route :to => :register_plugin,   :src => sockets[:router], :dest => sockets[:register],      :static => true
R.route :to => :register_service,  :src => sockets[:router], :dest => sockets[:register],      :static => true

# Scheduler -> Clients static route
R.route :from => :plugin_exec, :src => sockets[:plugin_exec], :dest => sockets[:router], :static => true

# Acks -> Clients
R.route :from => :acks, :src => sockets[:acks], :dest => sockets[:router], :static => true

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

