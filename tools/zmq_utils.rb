def socket_for_type_and_uri(ctx, socket_type, uri, opts = {})
  socket = ctx.socket(ZMQ.const_get("#{socket_type.to_s.upcase}"))

  # These aren't strictly necessary, but the behavior they enable is
  # what we usually expect.  For now, have all sockets use the same
  # options.  Set socket options *before* bind or connect.
  socket.setsockopt(ZMQ::LINGER, opts[:linger]) # flush messages before shutdown
  socket.setsockopt(ZMQ::HWM, opts[:hwm]) # high water mark, the number of buffered messages

  socket.bind uri

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
