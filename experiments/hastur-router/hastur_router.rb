#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'trollop'
require 'termite'
require 'pp'
require 'yajl' # fast JSON parser

opts = Trollop::options do
  opt :config,  "Configuration File", :type => String
end

if opts[:config] and not File.file?(opts[:config])
  Trollop::die :config, "--config <file> option is required!"
end

logger = Termite::Logger.new
ctx = ZMQ::Context.new(1)
poller = ZMQ::Poller.new

routes = {
  :client_in        => { :type => ZMQ::ROUTER, :uri => "tcp://*:20000" },
  :registration_out => { :type => ZMQ::PUSH,   :uri => "tcp://*:20001" },
  :heartbeat_out    => { :type => ZMQ::PUB,    :uri => "tcp://*:20002" },
  :notification_out => { :type => ZMQ::REQ,    :uri => "tcp://*:20003" },
  :stat_out         => { :type => ZMQ::PUB,    :uri => "tcp://*:20004" },
}

routes.each do |key, settings|
  logger << "setting up router socket '#{key.to_s}'"
  settings[:socket] = ctx.socket(settings[:type])
  settings[:socket].bind(settings[:uri])

  case settings[:type]
    when ZMQ::ROUTER, ZMQ::PUSH, ZMQ::PUB
      settings[:socket].setsockopt(ZMQ::HWM, 1)
      settings[:socket].setsockopt(ZMQ::LINGER, 1)
  end

  # only poll on reads; assume writes are always possible, or at least
  # always handled sanely by ZeroMQ
  poller.register(settings[:socket], ZMQ::POLLIN)
end

logger << "Sleeping for 1 second to allow ZMQ to fully initialize."
sleep 1

# stub
def router_error(messages, exception)
  pp messages
  abort(exception)
end

def route_client_message(sock)
  messages = []
  loop do
    STDOUT.write("!")
    sock.recv_string(msg = '', ZMQ::RCVMORE)
    messages << msg
    break unless sock.more_parts?
  end

  # make sure we're actually dealing with something remotely resembling JSON
  if messages[-1] !~ /^\s*{.*}\s*$/
    router_error(messages, Exception.new(:message => "Data part does not appear to contain JSON."))
    return
  end

  puts "M1: '#{messages[-1]}'"

  parser = Yajl::Parser.new :symbolize_keys => true

  begin
    parser.parse messages[-1] do |data|
      case data[:method]
        when /^register_/
          routes[:registration_out][:socket].send(messages)
        when /^stat_/
          routes[:stat_out][:socket].send(messages)
        when /^heartbeat/
          routes[:heartbeat_out][:socket].send(messages)
        when "notification"
          routes[:notification_out][:socket].send(messages)
      else
        router_error(messages, Exception.new(
          :message => "Unrecognized method '#{data[:method]}' in JSON, cannot route."
        ))
      end
    end
  rescue Exception => e
    router_error(messages, e)
  end
  #rescue Yajl::DecodeError => e
  #  router_error(messages, e)
  #end
end

loop do
  poller.poll(:blocking)

  poller.readables.each do |sock|
    STDOUT.write "-"
    if sock == routes[:client_in][:socket]
      STDOUT.write "+"
      route_client_message(sock)
    end
  end

  STDOUT.write "."
  sleep 0.2
end

