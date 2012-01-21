#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'trollop'
require 'uuid'

ZMQ_TYPELIST = ZMQ::SocketTypeNameMap.values.join(", ")

# default is: connect('tcp://localhost:5000') as ZMQ::SUB
opts = Trollop::options do
  banner <<-EOS
zmqcli.rb - a simple command-line utility for plumbing data into and out of ZeroMQ

Examples:
  # REQ / REP pairs with just zmqcli.rb
  zmqcli.rb --uri tcp://127.0.0.1:5000 --type req --bind
  zmqcli.rb --uri tcp://127.0.0.1:5000 --type rep --connect

  # ZeroMQ abstracts the direction of backing TCP connections
  zmqcli.rb --uri tcp://127.0.0.1:5000 --type req --connect
  zmqcli.rb --uri tcp://127.0.0.1:5000 --type rep --bind

  # PUSH / PULL, IPC, and read/write from files with one message per line
  zmqcli.rb --uri ipc:///tmp/zmqcli --type push --connect --infile messages.json
  zmqcli.rb --uri ipc:///tmp/zmqcli --type pull --bind    --outfile received.json

  Options:
EOS
  opt :uri,     "ZeroMQ URI",                                      :type => String,  :required => true
  opt :type,    "ZMQ Socket Type, one of: #{ZMQ_TYPELIST}",        :type => String,  :required => true
  opt :bind,    "bind()",           :default => false,             :type => :boolean
  opt :connect, "connect()",        :default => false,             :type => :boolean
  opt :linger,  "set ZMQ_LINGER",   :default => 1,                 :type => Integer
  opt :hwm,     "set ZMQ_HWM",      :default => 1,                 :type => Integer
  opt :id,      "set ZMQ_IDENTITY", :default => UUID.new.generate, :type => String
  opt :sleep,   "sleep seconds",    :default => 0.1,               :type => Float
  opt :spam,    "spam 1 msg",       :default => false,             :type => :boolean
  opt :infile,  "read from <filename> instead of STDIN",           :type => String
  opt :outfile, "append to <filename> instead of STDOUT",          :type => String
end

# further option handling / checking
if (opts[:bind].nil? and opts[:connect].nil?) or (opts[:bind] == opts[:connect])
  Trollop::die "Exactly one of --bind or --connect is required."
end

if opts[:uri].nil? or opts[:uri] !~ /\w+:\/\/[^:]+:\d+/
  Trollop::die :uri, "--uri is required and must be in protocol://hostname:port form"
end

if opts[:uri] =~ /\Wlocalhost\W/
  Trollop::die :uri, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
end

unless ZMQ::SocketTypeNameMap.has_value?(opts[:type].upcase)
  Trollop::die :type, "must be one of: #{ZMQ_TYPELIST}"
end

# ZeroMQ setup
ctx = ZMQ::Context.new(1)
socktype = ZMQ::SocketTypeNameMap.invert[opts[:type].upcase]
sock = ctx.socket(socktype)

if opts[:bind]
  sock.bind(opts[:uri])
  STDERR.puts "Listening on '#{opts[:uri]}'."
else
  sock.connect(opts[:uri])
  STDERR.puts "Connected to '#{opts[:uri]}'."
end

# these aren't strictly necessary, but the behavior they enable is what we usually expect
sock.setsockopt(ZMQ::LINGER,   opts[:linger]) # flush messages before shutdown
sock.setsockopt(ZMQ::HWM,      opts[:hwm])    # high-water mark # of buffered messages
sock.setsockopt(ZMQ::IDENTITY, opts[:id])     # useful for REQ and SUB, harmless elsehwere

# set up input/output from/to files or STDIN/STDOUT as directed by CLI
infile = STDIN
if not opts[:infile].nil?
  infile = File.new(opts[:infile], 'r')
  STDERR.puts "Data will be read from '#{opts[:infile]}' and sent."
end

outfile = STDOUT
if not opts[:outfile].nil?
  outfile = File.new(opts[:outfile], 'w+')
  STDERR.puts "Received data will be appended to '#{opts[:outfile]}'."
end

# ZMQ::REP, blocking loop
if socktype == ZMQ::REP
  while sock.recv_string(request = '')
    outfile.puts request
    STDERR.puts "Got request: '#{request}'"
    reply = infile.gets.chomp
    STDERR.puts "Sending response: '#{reply}'"
    sock.send_string(reply)
    if opts[:spam]
      infile.seek(0, IO::SEEK_SET)
    end
  end
# ZMQ::REQ, blocking loop
elsif socktype == ZMQ::REQ
  while request = infile.gets
    STDERR.puts "About to send '#{request}'"
    request.chomp!
    sock.send_string(request)
    STDERR.puts "Sent '#{request}'\nWaiting for response."
    sock.recv_string(reply)
    STDERR.puts "Got response: '#{reply}'"
    outfile.puts reply
    infile.seek(0, IO::SEEK_SET) if opts[:spam]
  end
# ZMQ::PUB / ZMQ::PUSH, blocking loop
elsif socktype == ZMQ::PUB or socktype == ZMQ::PUSH
  while data = infile.gets
    data.chomp!
    STDERR.puts "Sending: #{data}"
    sock.send_string(data)
    infile.seek(0, IO::SEEK_SET) if opts[:spam]
  end
# ZMQ::SUB / ZMQ::PULL, blocking loop
elsif socktype == ZMQ::SUB or socktype == ZMQ::PULL
  data = ""
  while sock.recv_string(data)
    outfile.puts data
  end
# DEALER / ROUTER?, poll-based
elsif socktype == ZMQ::DEALER or socktype == ZMQ::ROUTER
  select_in, _ = IO.select([infile], nil, nil, 0.1)
  abort "WTFBBQ: IO.select on input #{infile} failed" unless select_in

  poller = ZMQ::Poller.new

  if opts[:send]
    poller.register_writable(sock)
  elsif opts[:recv]
    poller.register_readable(sock)
  else
    abort "BUG!"
  end

  loop do
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
          infile.seek(0, IO::SEEK_SET) if opts[:spam]
        end
      end
    end

    sleep opts[:sleep]
    STDERR.write '.'
  end
end

