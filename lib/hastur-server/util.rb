# A collection of useful functions, including for the ffi-rzmq gem.

require "multi_json"
require "ffi-rzmq"
require "termite"

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

    def hastur_internal_logger
      @__hastur_internal_logger ||= Termite.logger(:component => "Hastur")
    end

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

    # This is not really thorough.  It's also just a true/false, and
    # doesn't try to check for things we fix up automatically.
    #
    # @param uri [String] The URI to check
    #
    def valid_zmq_uri?(uri)
      case uri
        when %r{ipc://.};         true
        when %r{tcp://[^:]+:\d+}; true
        else;                     false
      end
    end

    # Convert to a Hastur-usable URI.  This is called automatically on
    # URIs that we bind and connect on.  It's important because some
    # versions of ZeroMQ don't handle "*" or "localhost" properly as a
    # hostname, or have other slight quirks, and we don't know in
    # advance which version of ZeroMQ we're connecting to.
    #
    # @param uri [String] The URI to convert
    #
    def to_valid_zmq_uri(uri)
      match = uri.match %r{\A([a-zA-Z]{3,6})://([^/:]+)(:\d+)?(/.*)?\Z}
      raise "URI must be of the form: transport://hostname or transport://hostname/path" unless match
      protocol = match[1]  # example: "ipc"
      hostname = match[2]  # example: "subdomain.bob.com"
      port = match[3]      # example: ":374"
      path = match[4]      # example: "/parsnip_in_a/pear_tree"

      hostname = "0.0.0.0" if hostname == "localhost"
      hostname = "0.0.0.0" if hostname == "*"

      "#{protocol}://#{hostname}#{port}#{path}"
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
        hastur_internal_logger.error "Error setting ZMQ::LINGER: #{::ZMQ::Util.error_string}" unless rc > -1

        if ZMQ::LibZMQ.version2?
          rc = socket.setsockopt(::ZMQ::HWM, opts[:hwm]) if opts[:hwm]
        elsif ZMQ::LibZMQ.version3?
          rc = socket.setsockopt(::ZMQ::RCVHWM, opts[:hwm]) if opts[:hwm]
          socket.setsockopt(::ZMQ::SNDHWM, opts[:hwm]) if opts[:hwm] unless rc < 0
        end
        hastur_internal_logger.error "Error setting ZMQ::HWM: #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end

    def bind(socks, uri)
      [socks].flatten.each do |sock|
        rc = sock.bind(uri)
        hastur_internal_logger.error "Could not bind socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end

    def connect(socks, uri)
      [socks].flatten.each do |sock|
        rc = sock.connect(uri)
        hastur_internal_logger.error "Could not connect socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
      end
    end

    def read_msgs(socket)
      message = []
      rc = socket.recvmsgs message
      if ::ZMQ::Util.resultcode_ok? rc
        message
      else
        hastur_internal_logger.error "Could not read messages: #{::ZMQ::Util.error_string}"
        false
      end
    end

    def send_msgs(socket, message)
      rc = socket.sendmsgs message
      if ::ZMQ::Util.resultcode_ok? rc
        true
      else
        hastur_internal_logger.error "Could not send messages: #{::ZMQ::Util.error_string}"
        false
      end
    end

    def read_strings(socket)
      message = []
      rc = socket.recv_strings message
      if ::ZMQ::Util.resultcode_ok? rc
        message
      else
        hastur_internal_logger.error "Could not read strings: #{::ZMQ::Util.error_string}"
        false
      end
    end

    def send_strings(socket, message)
      rc = socket.send_strings message
      if ::ZMQ::Util.resultcode_ok? rc
        true
      else
        hastur_internal_logger.error "Could not send strings: #{::ZMQ::Util.error_string}"
        false
      end
    end

    #
    # Check a URI for validity before passing onto ZMQ.
    #
    def check_uri(uri)
      result = /\A(?<protocol>\w+):\/\/(?<hostname>[^:]+):(?<port>\d+)\Z/.match(uri)
      if result.nil?
        raise "URI's must be in: protocol://hostname:port format"
      end

      # We used to prevent using "localhost" here since it fails for some versions
      # of ZMQ with ipv6, but now we auto-convert to prevent problems.
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
      # Prevent modifying original opts object
      opts = opts.clone

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
      socket.setsockopt(::ZMQ::LINGER, opts[:linger]) if opts[:linger]

      # high water mark, the number of buffered messages
      if ZMQ::LibZMQ.version2?
        socket.setsockopt(::ZMQ::HWM, opts[:hwm]) if opts[:hwm]
      elsif ZMQ::LibZMQ.version3?
        socket.setsockopt(::ZMQ::RCVHWM, opts[:hwm]) if opts[:hwm]
        socket.setsockopt(::ZMQ::SNDHWM, opts[:hwm]) if opts[:hwm]
      end

      # Identity for router, req and sub sockets
      socket.setsockopt(::ZMQ::IDENTITY, opts[:identity]) if opts[:identity]

      status = 0
      if opts[:bind]
        ok = ZMQ::Util.resultcode_ok?(socket.bind uri)
        hastur_internal_logger.error "Error #{::ZMQ::Util.error_string} when binding socket to #{uri}!" unless ok
      elsif opts[:connect]
        if uri.respond_to?(:each)
          uri.each do |one_uri|
            rc = socket.connect one_uri
            hastur_internal_logger.error "Error #{::ZMQ::Util.error_string} when connecting socket to #{one_uri.inspect}!" if rc < 0
          end
        else
          rc = socket.connect uri
          hastur_internal_logger.error "Error #{::ZMQ::Util.error_string} when connecting socket to #{uri.inspect}!" if rc < 0
        end
      else
        raise "Must provide either bind or connect option to bind_or_connect_socket!"
      end
      socket
    end
  end
end
