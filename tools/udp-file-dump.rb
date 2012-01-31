#!/usr/bin/env ruby
#
# A simple program for recording packets on a UDP port to files.
# One of the primary use cases is to record packets to use as test input.
# Could also be handy to debug Hastur UDP clients.

require 'socket'
require 'trollop'

opts = Trollop::options do
  banner <<-EOS
#{$0} - dump UDP packets to files, one packet per file

Example:
  #{$0} --port 8125 --path /tmp/foo

EOS
  opt :bind, "address to bind to",          :type => String,  :default  => "127.0.0.1"
  opt :port, "UDP port to listen on",       :type => Integer, :default  => 8125
  opt :path, "directory to write files to", :type => String,  :required => true
end

counter=0

BasicSocket.do_not_reverse_lookup = true
client = UDPSocket.new
client.bind(opts[:bind], opts[:port])

loop do
  data, addr = client.recvfrom(16384)

  File.open("#{opts[:path]}/#{counter}", "w") do |f|
    f.write data
  end

  counter += 1
end

client.close

