#!/usr/bin/env ruby
# encoding: utf-8
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'trollop'
require 'rainbow'
require 'uuid'

require 'hastur-server/message'
require 'hastur-server/util'

ZMQ_TYPELIST = ZMQ::SocketTypeNameMap.values.join(", ")
MultiJson.engine = :yajl

# default is: connect('tcp://localhost:5000') as ZMQ::SUB
opts = Trollop::options do
  banner <<-EOS
msgtool.rb - a simple command-line utility for plumbing Hastur messages

Examples:
  # REQ / REP pairs with just msgtool.rb
  msgtool.rb --uri tcp://127.0.0.1:5000 --type req --bind
  msgtool.rb --uri tcp://127.0.0.1:5000 --type rep --connect

  # ZeroMQ abstracts the direction of backing TCP connections
  msgtool.rb --uri tcp://127.0.0.1:5000 --type req --connect
  msgtool.rb --uri tcp://127.0.0.1:5000 --type rep --bind

  # PUSH / PULL, IPC, and read/write from files with one message per line
  msgtool.rb --uri ipc:///tmp/msgtool --type push --connect --infile messages.json
  msgtool.rb --uri ipc:///tmp/msgtool --type pull --bind    --outfile received.json

  Options:
EOS
  opt :uri,       "ZeroMQ URI",                                       :type => String, :required => true
  opt :type,      "ZMQ Socket Type, one of: #{ZMQ_TYPELIST}",         :type => String, :required => true
  opt :bind,      "bind()",            :default => false,             :type => :boolean
  opt :connect,   "connect()",         :default => false,             :type => :boolean
  opt :linger,    "set ZMQ_LINGER",    :default => 1,                 :type => Integer
  opt :hwm,       "set ZMQ_HWM",       :default => 1,                 :type => Integer
  opt :id,        "set ZMQ_IDENTITY",  :default => UUID.new.generate, :type => String
  opt :send,      "send() - only for router or dealer sockets",       :type => :boolean
  opt :recv,      "recv() - only for router or dealer sockets",       :type => :boolean
  opt :sleep,     "sleep seconds",     :default => 0.1,               :type => Float
  opt :spam,      "spam 1 msg",        :default => false,             :type => :boolean
  opt :infile,    "read from <filename> instead of STDIN",            :type => String
  opt :outfile,   "append to <filename> instead of STDOUT",           :type => String
  opt :subscribe, "subscribe pattern", :default => "",                :type => String
  opt :normalize, "normalize JSON",    :default => false,             :type => :boolean
  opt :prefix,    "prefix string",     :default => "",                :type => String
  opt :color,     "enable colored output",                            :type => :boolean
  opt :envelope,  "envelope string",                                  :type => String, :multi => true
  opt :route,     "do Hastur client routing",                         :type => :boolean
end

# further option handling / checking
if (opts[:bind].nil? and opts[:connect].nil?) or (opts[:bind] == opts[:connect])
  Trollop::die "Exactly one of --bind or --connect is required."
end

if opts[:uri].nil? or opts[:uri] !~ /\w+:\/\/[^:]+:?\d*/
  Trollop::die :uri, "--uri is required and must be in protocol://hostname:port form"
end

if opts[:uri] =~ /\Wlocalhost\W/
  Trollop::die :uri, "Don't use 'localhost'. ZMQ 2.x will break silently around IPv6 localhost."
end

if opts[:subscribe] != "" && opts[:type].downcase != "sub"
  Trollop::die :subscribe, "You may only use option 'subscribe' with a socket of type sub!"
end

if (opts[:send] || opts[:recv]) && !["dealer", "router"].include?(opts[:type].downcase)
  Trollop::die :type, "You may only use --send or --recv with a dealer or router socket!"
end

unless ZMQ::SocketTypeNameMap.has_value?(opts[:type].upcase)
  Trollop::die :type, "must be one of: #{ZMQ_TYPELIST}"
end

def verbose(data)
  STDERR.puts data
end

# ZeroMQ setup
version_hash = ZMQ::LibZMQ.version
version = "#{version_hash[:major]}.#{version_hash[:minor]}p#{version_hash[:patch]}"
verbose "Using ZeroMQ version #{version}"

ctx = ZMQ::Context.new(1)
socktype = ZMQ::SocketTypeNameMap.invert[opts[:type].upcase]
sock = ctx.socket(socktype)

if opts[:bind]
  sock.bind(opts[:uri])
  verbose "Listening on '#{opts[:uri]}'."
else
  sock.connect(opts[:uri])
  verbose "Connected to '#{opts[:uri]}'."
end

# these aren't strictly necessary, but the behavior they enable is what we usually expect
sock.setsockopt(ZMQ::LINGER,    opts[:linger]) # flush messages before shutdown
sock.setsockopt(ZMQ::HWM,       opts[:hwm])    # high-water mark # of buffered messages
sock.setsockopt(ZMQ::IDENTITY,  opts[:id])     # useful for ROUTER, REQ and SUB, harmless elsewhere
sock.setsockopt(ZMQ::SUBSCRIBE, opts[:subscribe]) if socktype == ZMQ::SUB  # Subscribe to everything

# set up input/output from/to files or STDIN/STDOUT as directed by CLI
@infile = STDIN
if not opts[:infile].nil?
  @infile = File.new(opts[:infile], 'r')
  verbose "Data will be read from '#{opts[:infile]}' and sent."
end

@outfile = STDOUT
if not opts[:outfile].nil?
  @outfile = File.new(opts[:outfile], 'w+')
  verbose "Received data will be appended to '#{opts[:outfile]}'."
end

def verbose(dir, data)
  if data.kind_of? Hastur::Message::Base
    STDERR.puts "Message: #{data.to_json}"
  else
    STDERR.puts data
  end
end

def savemsg(msg)
  verbose :save, msg
  @outfile.puts msg.to_json
end

def loadmsg
  if opts[:spam]
    @infile.seek(0, IO::SEEK_SET)
  end
  json = @infile.gets.chomp
  verbose :load, json
  Hastur::Message.from_json json
end

# ZMQ::REP, blocking loop
if socktype == ZMQ::REP
  while msg = Hastur::Message.recv(sock)
    savemsg msg
    loadmsg.send(sock)
  end
# ZMQ::REQ, blocking loop
elsif socktype == ZMQ::REQ
  while loadmsg.send(sock)
    msg = Hastur::Message.recv(sock)
    savemsg(msg)
  end
# ZMQ::PUB / ZMQ::PUSH, blocking loop
elsif socktype == ZMQ::PUB or socktype == ZMQ::PUSH
  loop do
    loadmsg.send(sock)
  end
# ZMQ::SUB / ZMQ::PULL, blocking loop
elsif socktype == ZMQ::SUB or socktype == ZMQ::PULL
  while msg = Hastur::Message.recv(sock)
    savemsg(msg)
  end
# DEALER / ROUTER?, poll-based
elsif socktype == ZMQ::DEALER or socktype == ZMQ::ROUTER
  poller = ZMQ::Poller.new
  poller.register_readable(sock)

  loop do
    poller.poll(0.1)

    poller.readables.each do |sock|
      msg = Hastur::Message.recv(sock)
      savemsg(msg)
    end

    select_in, _ = IO.select([@infile], nil, nil, 0.1)

    if select_in && select_in[0]
      json = select_in[0].gets
      msg = Hastur::Message.from_json(json)
      msg.send(sock)
      @infile.seek(0, IO::SEEK_SET) if opts[:spam]
    end
  end
end

