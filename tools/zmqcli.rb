#!/usr/bin/env ruby

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'uuid'

require_relative "zmq_utils"

ZMQ_TYPELIST = ZMQ::SocketTypeNameMap.values.join(", ")

MultiJson.engine = :yajl

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
  opt :uri,       "ZeroMQ URI",                                      :type => String, :required => true
  opt :type,      "ZMQ Socket Type, one of: #{ZMQ_TYPELIST}",        :type => String, :required => true
  opt :bind,      "bind()",           :default => false,             :type => :boolean
  opt :connect,   "connect()",        :default => false,             :type => :boolean
  opt :linger,    "set ZMQ_LINGER",   :default => 1,                 :type => Integer
  opt :hwm,       "set ZMQ_HWM",      :default => 1,                 :type => Integer
  opt :id,        "set ZMQ_IDENTITY", :default => UUID.new.generate, :type => String
  opt :send,      "send() - only for router or dealer sockets",      :type => :boolean
  opt :recv,      "recv() - only for router or dealer sockets",      :type => :boolean
  opt :sleep,     "sleep seconds",    :default => 0.1,               :type => Float
  opt :spam,      "spam 1 msg",       :default => false,             :type => :boolean
  opt :infile,    "read from <filename> instead of STDIN",           :type => String
  opt :outfile,   "append to <filename> instead of STDOUT",          :type => String
  opt :subscribe, "subscribe pattern",:default => "",                :type => String
  opt :normalize, "normalize JSON",   :default => false,             :type => :boolean
  opt :prefix,    "prefix string",    :default => "",                :type => String
  opt :envelope,  "envelope string",                                 :type => String, :multi => true
end

PREFIX = opts[:prefix]
ENVELOPE = opts[:envelope]
NORMALIZE = opts[:normalize]

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

def to_console(data)
  STDERR.puts PREFIX + " " + data
end

# ZeroMQ setup
version_hash = ZMQ::LibZMQ.version
version = "#{version_hash[:major]}.#{version_hash[:minor]}p#{version_hash[:patch]}"
to_console "Using ZeroMQ version #{version}"

ctx = ZMQ::Context.new(1)
socktype = ZMQ::SocketTypeNameMap.invert[opts[:type].upcase]
sock = ctx.socket(socktype)

if opts[:bind]
  sock.bind(opts[:uri])
  to_console "Listening on '#{opts[:uri]}'."
else
  sock.connect(opts[:uri])
  to_console "Connected to '#{opts[:uri]}'."
end

# these aren't strictly necessary, but the behavior they enable is what we usually expect
sock.setsockopt(ZMQ::LINGER,    opts[:linger]) # flush messages before shutdown
sock.setsockopt(ZMQ::HWM,       opts[:hwm])    # high-water mark # of buffered messages
sock.setsockopt(ZMQ::IDENTITY,  opts[:id])     # useful for REQ and SUB, harmless elsehwere
sock.setsockopt(ZMQ::SUBSCRIBE, opts[:subscribe]) if socktype == ZMQ::SUB  # Subscribe to everything

# set up input/output from/to files or STDIN/STDOUT as directed by CLI
infile = STDIN
if not opts[:infile].nil?
  infile = File.new(opts[:infile], 'r')
  to_console "Data will be read from '#{opts[:infile]}' and sent."
end

outfile = STDOUT
if not opts[:outfile].nil?
  outfile = File.new(opts[:outfile], 'w+')
  to_console "Received data will be appended to '#{opts[:outfile]}'."
end

def send_string(sock, data)
  if NORMALIZE
    # Decode, re-encode
    hash = MultiJson.decode(data)
    data = MultiJson.encode(hash)
  end

  messages = ENVELOPE + [ data ]

  to_console "Sending message(s): #{messages.inspect}"
  multi_send sock, messages
end

def recv_string(sock)
  data = multi_recv(sock)
end

# ZMQ::REP, blocking loop
if socktype == ZMQ::REP
  while request = recv_string(sock)
    outfile.puts request[-1]
    to_console "Got request: '#{request.inspect}'"
    reply = infile.gets.chomp
    to_console "Sending response: '#{reply}'"
    send_string(socket, reply)
    if opts[:spam]
      infile.seek(0, IO::SEEK_SET)
    end
  end
# ZMQ::REQ, blocking loop
elsif socktype == ZMQ::REQ
  while request = infile.gets
    to_console "About to send '#{request}'"
    request.chomp!
    send_string(socket, request)
    to_console "Sent '#{request}'\nWaiting for response."
    reply = recv_string(sock)
    to_console "Got response: '#{reply.inspect}'"
    outfile.puts reply[-1]
    infile.seek(0, IO::SEEK_SET) if opts[:spam]
  end
# ZMQ::PUB / ZMQ::PUSH, blocking loop
elsif socktype == ZMQ::PUB or socktype == ZMQ::PUSH
  while data = infile.gets
    data.chomp!
    to_console "Sending: #{data}"
    send_string(socket, data)
    infile.seek(0, IO::SEEK_SET) if opts[:spam]
  end
# ZMQ::SUB / ZMQ::PULL, blocking loop
elsif socktype == ZMQ::SUB or socktype == ZMQ::PULL
  data = ""
  while data = recv_string(sock)
    outfile.puts data[-1]
    to_console "Received: #{data.inspect}"
  end
# DEALER / ROUTER?, poll-based
elsif socktype == ZMQ::DEALER or socktype == ZMQ::ROUTER
  poller = ZMQ::Poller.new

  if opts[:send]
    poller.register_writable(sock)
  elsif opts[:recv]
    poller.register_readable(sock)
  else
    abort "You have set neither --send or --recv for a bidirectional socket type.\nYou bastard."
  end

  loop do
    poller.readables.each do |sock|
      STDERR.write '+'
      data = recv_string(sock)
      outfile.puts data[-1]
      to_console "Received: #{data.inspect}"
    end

    poller.writables.each do |sock|
      select_in, _ = IO.select([infile], nil, nil, 0.1)
      if select_in && select_in[0]
        if line = select_in[0].gets
          STDERR.write '-'
          send_string(sock, line.chomp)
          infile.seek(0, IO::SEEK_SET) if opts[:spam]
        end
      end
    end

    sleep opts[:sleep]
    STDERR.write '.'
    poller.poll_nonblock  # Could also use the sleep interval, maybe
  end
end

