#!/usr/bin/env ruby -r socket

# Netcat doesn't send multiple datagrams

if ARGV.size != 3
  raise "Usage: #{$0} <filename> <hostname> <port>"
end

File.read(ARGV[0]).each_line { |line| UDPSocket.new.send line, 0, ARGV[1], ARGV[2].to_i }
