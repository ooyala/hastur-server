# A collection of useful functions for working with ZeroMQ, specifically with
# the ffi-rzmq gem.
#
require "multi_json"
require "ffi-rzmq"

module Hastur
  module ZMQUtils
    # 
    # Check a URI for validity before passing onto ZMQ.
    # We explicitly disallow "localhost" because ZMQ will break silently on IPv6 enabled systems.
    #
    def check_uri(uri)
      result = /\A(?<protocol>\w+):\/\/(?<hostname>[^:]+):(?<port>\d+)\Z/.match(uri)
      if result.nil?
        raise "URI's must be in: protocol://hostname:port format"
      end

      if result[:hostname] == "localhost"
        raise "'localhost' is not allowed, since ZMQ will silently fail on IPv6-enabled hosts"
      end
    end

    #
    # Create a socket and bind in one go, setting sane defaults for sockopts.
    # Defaults:
    # * ZMQ::LINGER => 1
    # * ZMQ::HWM    => 1
    #
    # Example:
    #  Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PULL, "tcp://127.0.0.1:1234")
    #
    def bind_socket(ctx, type, uri, opts = {})
      socket = ctx.socket(type)

      opts[:linger] = 1 unless opts.has_key?(:linger)
      opts[:hwm]    = 1 unless opts.has_key?(:hwm)

      # Linger and HWM aren't strictly necessary, but the behavior
      # they enable is what we usually expect.  For now, have all
      # sockets use the same options.  Set socket options *before*
      # bind or connect.

      # flush messages before shutdown
      socket.setsockopt(ZMQ::LINGER, opts[:linger]) if opts[:linger]
      # high water mark, the number of buffered messages
      socket.setsockopt(ZMQ::HWM,    opts[:hwm])    if opts[:hwm]

      socket.bind uri
      STDERR.puts "New #{socket_type} socket listening on '#{uri}'."
      socket
    end
    
    #
    # receive a multi-part message in one go as an array
    #
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
    
    #
    # Send a multi-part message using an array.
    #
    def multi_send(socket, messages)
      last_message = messages[-1]
      (messages[0..-2]).each do |message|
        # I know you can't resend a 0mq message...  Does ffi-rzmq shield us from that
        # or do we need to dup?
        socket.send_string(message.dup, ZMQ::SNDMORE)
      end
      socket.send_string(last_message)
    end
    
    #
    # Send a Hastur-specific message with a header envelope containing version, time, and sequence.
    #
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
  end
end
