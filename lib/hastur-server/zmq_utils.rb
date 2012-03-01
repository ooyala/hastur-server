# A collection of useful functions for working with ZeroMQ, specifically with
# the ffi-rzmq gem.
#
require "multi_json"
require "ffi-rzmq"

module Hastur
  module ZMQUtils
    extend self  # Allows calling as ZMQUtils.blah

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
    # Create a socket and connect in one go, setting sane defaults for sockopts.
    # Defaults:
    # * ZMQ::LINGER => 1
    # * ZMQ::HWM    => 1
    #
    # Example:
    #  Hastur::ZMQUtils.connect_socket(ctx, ZMQ::PUSH, "tcp://127.0.0.1:1234")
    #
    def connect_socket(ctx, type, uri, opts = {})
      bind_or_connect_socket(ctx, type, uri, opts.merge(:connect => true, :bind => false))
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
      bind_or_connect_socket(ctx, type, uri, opts.merge(:connect => false, :bind => true))
    end

    #
    # Send a Hastur-specific message with a header envelope containing version, time, and sequence.
    #
    def hastur_send(socket, method, data_hash)
      method ||= "error"
      @seq_num ||= 0
      @uptime ||= Time.now.to_i
      # TODO(noah): add client UUID here
      packet_data = {
        'sequence' => @seq_num,
        'uptime' => @uptime,
        'time' => Time.now,
      }
      method_data = { 'method' => method } if data_hash[:method].nil?
      @seq_num += 1
      json = MultiJson.encode(data_hash.merge(packet_data).merge(method_data || {}))
      envelope = [ "v1\n#{method}\nack:none" ]
      socket.send_strings(envelope + [ json ])
    end

    private

    #
    # Create a socket and bind or connect in one go, setting sane defaults for sockopts.
    # Defaults:
    # * ZMQ::LINGER => 1
    # * ZMQ::HWM    => 1
    #
    # Example:
    #  Hastur::ZMQUtils.bind_socket(ctx, ZMQ::PULL, "tcp://127.0.0.1:1234")
    #
    def bind_or_connect_socket(ctx, type, uri, opts = {})
      if type.kind_of?(Symbol) || type.kind_of?(String)
        type = ZMQ.const_get(type.to_s.upcase)
      end

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
      # Identity for router, req and sub sockets
      socket.setsockopt(ZMQ::IDENTITY, opts[:identity]) if opts[:identity]

      status = 0
      if opts[:bind]
        status = socket.bind uri
      elsif opts[:connect]
        if uri.respond_to?(:each)
          uri.each do |single_uri|
            status = socket.connect single_uri
            break if status < 0
          end
        else
          status = socket.connect uri
        end
      else
        raise "Must provide either bind or connect option to bind_or_connect_socket!"
      end
      socket
    end
  end
end
