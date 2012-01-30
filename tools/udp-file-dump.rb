#!/usr/bin/env ruby
#
# a simple program that reads UDP port 8125 and records each
# packet to a file unaltered
#
# mostly useful for writing out actual data for testing parsers, etc.
# like Hastur::Input::Collectd

require 'socket'
counter=0

BasicSocket.do_not_reverse_lookup = true
client = UDPSocket.new
client.bind('127.0.0.1', 8125)

loop do
  data, addr = client.recvfrom(1500)

  File.open("/tmp/collectd-packets/#{counter}", "w") do |f|
    f.write data
  end
  counter += 1
end

client.close

