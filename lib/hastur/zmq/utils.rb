require "multi_json"

module Hastur
  module ZMQ
    module Utils
      STATSD_RE = %r{
        \A\s*               # start of string, any amount of whitespace 
        (?<name>[-\.\w]+)   # stat name, letters, numbers, ., _, and - are allowed
        :                   # : separator
        (?<value>[\.\d]+)   # a number, integer or floating point
        \|                  # | separator
        (?<unit>\p{Graph}+) # the unit, e.g. "c" or "ms", but could have |@\d\.\d but don't parse that yet
        \s*\Z               # any amount of whitespace, end of string
      }xn
    end
  end
end

def socket_for_type_and_uri(ctx, socket_type, uri, opts = {})
  socket = ctx.socket(ZMQ.const_get("#{socket_type.to_s.upcase}"))
  # These aren't strictly necessary, but the behavior they enable is
  # what we usually expect.  For now, have all sockets use the same
  # options.  Set socket options *before* bind or connect.
  socket.setsockopt(ZMQ::LINGER, opts[:linger]) # flush messages before shutdown
  socket.setsockopt(ZMQ::HWM, opts[:hwm]) # high water mark, the number of buffered messages
  if opts[:connect]
    socket.connect uri
  else
    socket.bind uri
  end
  STDERR.puts "New #{socket_type} socket listening on '#{uri}'."
  socket
end

def multi_recv(socket)
  messages = []
  socket.recv_string(data = "")
  messages << data
  while socket.more_parts?
    socket.recv_string(data = "")
    messages << data
  end
  messages
end

def multi_send(socket, messages)
  last_message = messages[-1]
  (messages[0..-2]).each do |message|
    # I know you can't resend a 0mq message...  Does ffi-rzmq shield us from that
    # or do we need to dup?
    socket.send_string(message.dup, ZMQ::SNDMORE)
  end
  socket.send_string(last_message)
end

def hastur_send(socket, method, data_hash)
  method ||= "error"
  @seq_num ||= 0
  @uptime ||= Time.now.to_i
  packet_data = {
    'method' => method,
    'sequence' => @seq_num,
    'uptime' => @uptime,
    'time' => Time.now,
  }
  @seq_num += 1
  json = MultiJson.encode(data_hash.merge(packet_data))
  envelope = [ "v1\n#{method}\nack:none" ]
  multi_send(socket, envelope + [ json ])
end
