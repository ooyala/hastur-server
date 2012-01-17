#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'pp'
require 'trollop'

# default is: connect('tcp://localhost:5000') as ZMQ::SUB
opts = Trollop::options do
  opt :uri,     "ZeroMQ URI",    :default => "tcp://localhost:5000"
  opt :bind,    "bind()",        :default => false
  opt :connect, "connect()",     :default => false
  opt :req,     "ZMQ_REQ",       :default => false
  opt :rep,     "ZMQ_REP",       :default => false
  opt :sub,     "ZMQ_SUB",       :default => false
  opt :pub,     "ZMQ_PUB",       :default => false
  opt :dealer,  "ZMQ_DEALER",    :default => false
  opt :router,  "ZMQ_ROUTER",    :default => false
  opt :push,    "ZMQ_PUSH",      :default => false
  opt :pull,    "ZMQ_PULL",      :default => false
  opt :sleep,   "sleep seconds", :default => 0.1,         :type => Float
  opt :infile,  "read from <filename> instead of STDIN",  :type => String
  opt :outfile, "append to <filename> instead of STDOUT", :type => String
end

Trollop::die "--bind and --connect are mutually exclusive" if opts[:bind] and opts[:connect]
Trollop::die "Exactly one of --bind or --connect is required." if not (opts[:bind] or opts[:connect])

ctx = ZMQ::Context.new(1)

socktype = nil
if opts[:sub]
  socktype = ZMQ::SUB
  opts[:send] = false
  opts[:recv] = true
  STDERR.puts "Socket type will be ZMQ::SUB"
elsif opts[:pub]
  socktype = ZMQ::PUB
  opts[:send] = true
  opts[:recv] = false
  STDERR.puts "Socket type will be ZMQ::PUB"
elsif opts[:dealer]
  socktype = ZMQ::DEALER
  opts[:send] = true
  opts[:recv] = true
  STDERR.puts "Socket type will be ZMQ::DEALER"
elsif opts[:router]
  socktype = ZMQ::ROUTER
  opts[:send] = true
  opts[:recv] = true
  STDERR.puts "Socket type will be ZMQ::ROUTER"
elsif opts[:req]
  socktype = ZMQ::REP
  opts[:send] = true
  opts[:recv] = true
  STDERR.puts "Socket type will be ZMQ::REP"
elsif opts[:req]
  socktype = ZMQ::REQ
  opts[:send] = true
  opts[:recv] = true
  STDERR.puts "Socket type will be ZMQ::REQ"
elsif opts[:push]
  socktype = ZMQ::PUSH
  opts[:send] = true
  opts[:recv] = false
  STDERR.puts "Socket type will be ZMQ::PUSH"
elsif opts[:pull]
  socktype = ZMQ::PULL
  opts[:send] = false
  opts[:recv] = true
  STDERR.puts "Socket type will be ZMQ::PULL"
else
  Trollop::die "Must select exactly one socket type."
end

sock = ctx.socket(socktype)

if opts[:bind]
  sock.bind(opts[:uri])
else
  sock.connect(opts[:uri])
end

infile = STDIN
if not opts[:infile].nil?
  infile = File.new(opts[:infile], 'r')
  STDERR.puts "Data will be read from #{opts[:outfile]} and sent."
end

outfile = STDOUT
if not opts[:outfile].nil?
  outfile = File.new(opts[:outfile], 'w+')
  STDERR.puts "Received data will be appended to #{opts[:outfile]}."
end

poller = ZMQ::Poller.new

if opts[:send]
  poller.register_writable(sock)
elsif opts[:recv]
  poller.register_readable(sock)
else
  abort "BUG!"
end

# REP
if opts[:rep]
  while sock.recv_string(request)
    outfile.puts request
    reply = infile.gets.chomp
    sock.send_string(reply)
  end
# REQ
elsif opts[:req]
  while request = infile.gets.chomp
    sock.send_string(request)
    sock.recv_string(reply)
    outfile.puts reply
  end
# PUB / PUSH
elsif opts[:pub] or opts[:push]
  while data = infile.gets.chomp
    sock.send_string(data)
  end
# SUB / PULL
elsif opts[:sub] or opts[:pull]
  while sock.recv_string(data)
    outfile.puts data
  end
# DEALER / ROUTER?
elsif opts[:dealer] or opts[:router]
  select_in, _ = IO.select([infile], nil, nil, 0.1)
  abort "WTFBBQ: IO.select on input #{infile} failed" unless select_in

  loop do
    poller.poll_nonblock

    poller.readables.each do |sock|
      STDERR.write '+'
      sock.recv_string(data)
      outfile.puts data
    end
    
    poller.writables.each do |sock|
      if select_in[0]
        if line = infile.gets
          STDERR.write '-'
          sock.send_string(line.chomp)
        end
      end
    end

    sleep opts[:sleep]
    STDERR.write '.'
  end
end

