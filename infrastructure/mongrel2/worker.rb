#!/usr/bin/env ruby
require 'ffi-rzmq'
require 'multi_json'

handler_thread = Thread.new do
  handler_ctx = ZMQ::Context.new(1)

  receive_queue = handler_ctx.socket(ZMQ::PULL)
  receive_queue.connect("tcp://127.0.0.1:9999")

  response_publisher = handler_ctx.socket(ZMQ::PUB)
  response_publisher.connect("tcp://127.0.0.1:9998")
  response_publisher.setsockopt(ZMQ::IDENTITY, "82209006-86FF-4982-B5EA-D1E29E55D481")

  stop_queue = handler_ctx.socket(ZMQ::PULL)
  stop_queue.connect("ipc://shutdown_queue")

  stopped = false
  poller = ZMQ::Poller.new
  poller.register_readable(receive_queue)
  poller.register_readable(stop_queue)

  until stopped do
    poller.poll
    if poller.readables.include?(stop_queue)
      stop_queue.close
      receive_queue.close
      response_publisher.close
      handler_ctx.terminate
      stopped = true
    else
      # Request comes in as "UUID ID PATH SIZE:HEADERS,SIZE:BODY,"
      rc = receive_queue.recv_string(data = "")
      raise "Error receiving from queue!" if rc < 0
      sender_uuid, client_id, request_path, request_message = data.split(' ', 4)
      len, rest = request_message.split(':', 2)
      headers = MultiJson.decode(rest[0...len.to_i])
      len, rest = rest[(len.to_i+1)..-1].split(':', 2)
      body = rest[0...len.to_i]

      if headers['METHOD'] == 'JSON' and MultiJson.decode(body)['type'] == 'disconnect'
        next # A client has disconnected, might want to do something here...
      end

      # Response goes out as "UUID SIZE:ID ID ID, BODY"
      content_body = "Hello world!"
      response_value = "#{sender_uuid} 1:#{client_id}, HTTP/1.1 200 OK\r\nContent-Length: #{content_body.size}\r\n\r\n#{content_body}"
      response_publisher.send_string(response_value)
    end
  end
end

ctx = ZMQ::Context.new(1)
stop_push_queue = ctx.socket(ZMQ::PUSH)
trap('INT') do # Send a message to shutdown on SIGINT
  stop_push_queue.bind("ipc://shutdown_queue")
  stop_push_queue.send_string("shutdown")
end

handler_thread.join

stop_push_queue.close
