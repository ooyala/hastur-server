#!/usr/bin/env ruby

require "rubygems"
require "socket"
require "trollop"

opts = Trollop::options do
  banner <<EOS
Send a JSON input file to a UDP socket.

Options:
EOS
  opt :hostname, "UDP hostname",                          :required => true, :type => String
  opt :infile,   "Input JSON file",                       :required => true, :type => String
  opt :port,     "UDP port number",    :default => 8125,                     :type => :int
end

socket = UDPSocket.new

STDERR.puts "Sending file '#{opts[:infile]}' to UDP socket #{opts[:hostname]}:#{opts[:port]}"

json_file = File.read(opts[:infile])
json_file.each_line do |line|
  socket.send line, 0, opts[:hostname], opts[:port]
end
