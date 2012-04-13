# A collection of useful functions, including thise for
# working with ZeroMQ, specifically with the ffi-rzmq gem.
require "multi_json"
require "ffi-rzmq"
module Hastur
  module Util
    extend self  # Allows calling as Util.blah

    SECS_2100       = 4102444800
    MILLI_SECS_2100 = 4102444800000
    MICRO_SECS_2100 = 4102444800000000
    NANO_SECS_2100  = 4102444800000000000
    SECS_1971       = 31536000
    MILLI_SECS_1971 = 31536000000
    MICRO_SECS_1971 = 31536000000000
    NANO_SECS_1971  = 31536000000000000

    #
    # Best effort to make all timestamps be Hastur timestamps, 64 bit
    # numbers that represent the total number of microseconds since Jan
    # 1, 1970 at midnight UTC.  Default to giving Time.now as a Hastur
    # timestamp.
    #
    def timestamp(ts=Time.now)
      case ts
        when nil, ""
          (Time.now.to_f * 1_000_000).to_i
        when Time;
          (ts.to_f * 1_000_000).to_i
        when SECS_1971..SECS_2100
          ts * 1_000_000
        when MILLI_SECS_1971..MILLI_SECS_2100
          ts * 1_000
        when MICRO_SECS_1971..MICRO_SECS_2100
          ts
        when NANO_SECS_1971..NANO_SECS_2100
          ts / 1_000
        else
          raise "Unable to convert timestamp: #{ts} (class: #{ts.class})"
      end
    end

    # application boot time in epoch microseconds, intentionally not system boot time
    BOOT_TIME = timestamp

    #
    # return the current uptime in microseconds
    #
    def uptime(time=Time.now)
      now = timestamp(time)
      time - BOOT_TIME
    end

    #
    # keep a single, global counter for the :sequence field
    #
    @counter = 0
    def next_seq
      @counter+=1
    end

    UUID_RE = /\A[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z/i

    def valid_uuid?(uuid)
      if UUID_RE.match(uuid)
        true
      else
        false
      end
    end

    # not really thorough yet
    def valid_zmq_uri?(uri)
      case uri
        when %r{ipc://.};         true
        when %r{tcp://[^:]+:\d+}; true
        else;                     false
      end
    end

    #
    # Find a useful socket identity, falling back to the memory address of the socket
    # if it no identity was assigned with setsockopt(ZMQ::IDENTITY, ""). Since identity
    # can't be changed after connect/bind, it's unlikely this will ever change during runtime.
    # @param [ZMQ::Socket] socket to identify
    # @return [String] id
    #
    def sockid(socket)
      if socket.kind_of? ZMQ::Socket
        rc = socket.getsockopt(ZMQ::IDENTITY, id=[])
        if ZMQ::Util.resultcode_ok?(rc) and id[0]
          id[0]
        else
          socket.socket.address
        end
      elsif socket.kind_of? FFI::Pointer
        socket.address
      else
        raise ArgumentError.new "Cannot generate a useful identity for the socket."
      end
    end

    def setsockopts(socks, opts = {})
      [socks].flatten.each do |sock|
        rc = sock.setsockopt(::ZMQ::LINGER, opts[:linger] || -1)
        raise "Error setting ZMQ::LINGER: #{::ZMQ::Util.error_string}" unless rc > -1
        rc = sock.setsockopt(::ZMQ::HWM, opts[:hwm] || 1)
        raise "Error setting ZMQ::HWM: #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end

    def bind(socks, uri)
      [socks].flatten.each do |sock|
        rc = sock.bind(uri)
        raise "Could not bind socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end

    def connect(socks, uri)
      [socks].flatten.each do |sock|
        rc = sock.connect(uri)
        raise "Could not connect socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end

    def read_msgs(socket)
      message = []
      rc = socket.recvmsgs message
      if ::ZMQ::Util.resultcode_ok? rc
        message
      else
        send_error ::ZMQ::Util.error_string
        false
      end
    end

    def send_msgs(socket, message)
      rc = socket.sendmsgs message
      if ::ZMQ::Util.resultcode_ok? rc
        true
      else
        send_error ::ZMQ::Util.error_string
        false
      end
    end

    def read_strings(socket)
      message = []
      rc = socket.recv_strings message
      if ::ZMQ::Util.resultcode_ok? rc
        message
      else
        send_error ::ZMQ::Util.error_string
        false
      end
    end

    def send_strings(socket, message)
      rc = socket.send_strings message
      if ::ZMQ::Util.resultcode_ok? rc
        true
      else
        send_error ::ZMQ::Util.error_string
        false
      end
    end

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
    #  Hastur::Util.connect_socket(ctx, ZMQ::PUSH, "tcp://127.0.0.1:1234")
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
    #  Hastur::Util.bind_socket(ctx, ZMQ::PULL, "tcp://127.0.0.1:1234")
    #
    def bind_socket(ctx, type, uri, opts = {})
      bind_or_connect_socket(ctx, type, uri, opts.merge(:connect => false, :bind => true))
    end

    private

    #
    # Create a socket and bind or connect in one go, setting sane defaults for sockopts.
    # Defaults:
    # * ZMQ::LINGER => 1
    # * ZMQ::HWM    => 1
    #
    # Example:
    #  Hastur::Util.bind_socket(ctx, ZMQ::PULL, "tcp://127.0.0.1:1234")
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
        ok = ZMQ::Util.resultcode_ok?(socket.bind uri)
        raise "Error #{::ZMQ::Util.error_string} when binding socket to #{uri}!" unless ok
      elsif opts[:connect]
        if uri.respond_to?(:each)
          uri.each do |one_uri|
            rc = socket.connect one_uri
            raise "Error #{::ZMQ::Util.error_string} when connecting socket to #{one_uri.inspect}!" if rc < 0
          end
        else
          rc = socket.connect uri
          raise "Error #{::ZMQ::Util.error_string} when connecting socket to #{uri.inspect}!" if rc < 0
        end
      else
        raise "Must provide either bind or connect option to bind_or_connect_socket!"
      end
      socket
    end
  end
end
