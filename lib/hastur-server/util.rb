# A collection of useful functions, including for the ffi-rzmq gem.

require "multi_json"
#require "ffi-rzmq"
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

    def hastur_logger
      @__hastur_internal_logger ||= ::Termite::Logger.new(:component => "Hastur")
    end

    def zmq_error
      ::ZMQ::Util.error_string
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

    #
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

    SUPPORTED_PROTOCOLS = [ "tcp", "inproc", "ipc" ]

    #
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

      unless SUPPORTED_PROTOCOLS.include?(protocol)
        raise "Unsupported protocol! (must be in #{SUPPORTED_PROTOCOLS.inspect})"
      end

      hostname = "127.0.0.1" if hostname == "localhost"
      hostname = "0.0.0.0" if hostname == "*" && protocol == "tcp"

      "#{protocol}://#{hostname}#{port}#{path}"
    end

    #
    # Find a useful socket identity, falling back to the memory address of the socket
    # if it no identity was assigned with setsockopt(ZMQ::IDENTITY, ""). Since identity
    # can't be changed after connect/bind, it's unlikely this will ever change during runtime.
    #
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

    #
    # Set a variety of socket options in a ZMQ-version-appropriate way.
    #
    # @param opts [Hash] an Options array
    # @option opts [String] :linger The socket linger option
    # @option opts [String] :hwm The socket high water mark for send and receive
    # @option opts [String] :identity The socket identity for router, req and sub sockets
    #
    def setsockopts(socks, opts = {})
      [socks].flatten.each do |sock|
        rc = sock.setsockopt(::ZMQ::LINGER, opts[:linger] || -1)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Error setting ZMQ::LINGER: #{zmq_error}" unless ok

        if ZMQ::LibZMQ.version2?
          rc = sock.setsockopt(::ZMQ::HWM, opts[:hwm]) if opts[:hwm]
        elsif ZMQ::LibZMQ.version3?
          rc = sock.setsockopt(::ZMQ::RCVHWM, opts[:hwm]) if opts[:hwm]
          ok = ZMQ::Util.resultcode_ok?(rc)
          rc = sock.setsockopt(::ZMQ::SNDHWM, opts[:hwm]) if opts[:hwm] && ok
        end
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Error setting ZMQ::HWM: #{zmq_error}" unless ok

        rc = sock.setsockopt(::ZMQ::IDENTITY, opts[:identity]) if opts[:identity]
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Error setting ZMQ::IDENTITY: #{zmq_error}" unless ok
      end
    end

    def bind(socks, uri)
      [socks].flatten.each do |sock|
        rc = sock.bind(uri)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Could not bind socket to URI '#{uri}': #{zmq_error}" unless ok
      end
    end

    def connect(socks, uri)
      [socks].flatten.each do |sock|
        rc = sock.connect(uri)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Could not connect socket to URI '#{uri}': #{zmq_error}" unless ok
      end
    end

    def read_msgs(socket)
      message = []
      rc = socket.recvmsgs message
      if ZMQ::Util.resultcode_ok?(rc)
        message
      else
        hastur_logger.error "Could not read messages: #{zmq_error}"
        false
      end
    end
    alias recv_msgs read_msgs

    def send_msgs(socket, message)
      rc = socket.sendmsgs message
      if ZMQ::Util.resultcode_ok?(rc)
        true
      else
        hastur_logger.error "Could not send messages: #{zmq_error}"
        false
      end
    end

    #
    # Read an array of Strings on a ZeroMQ socket.
    #
    # @param socket [ZMQ::Socket] The socket
    # @return [Array or false] The array of strings from a multipart message or false for failure.
    #
    def read_strings(socket)
      message = []
      rc = socket.recv_strings message
      if ZMQ::Util.resultcode_ok?(rc)
        message
      else
        hastur_logger.error "Could not read strings: #{zmq_error}"
        false
      end
    end
    # Also allow ZeroMQ naming
    alias recv_strings read_strings

    #
    # Send an array of Strings on a ZeroMQ socket.
    #
    # @param socket [ZMQ::Socket] The socket
    # @param message [Array of String] The Strings to send
    # @return [Boolean] Whether the send succeeded
    #
    def send_strings(socket, message)
      rc = socket.send_strings message
      if ZMQ::Util.resultcode_ok?(rc)
        true
      else
        hastur_logger.error "Could not send strings: #{zmq_error}"
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
    # @example Connect on loopback port 1234 with a PUSH socket
    #  Hastur::Util.connect_socket(ctx, ZMQ::PUSH, "tcp://127.0.0.1:1234")
    #
    # @param ctx [::ZMQ::Context] ZeroMQ Context
    # @param type The ZeroMQ socket type, like ZMQ::PULL, or a symbol like :PULL
    # @param uri The ZeroMQ URI, or an Array of same
    # @param opts [Hash] Options
    # @option opts [Fixnum] :linger The number of seconds to linger for the ZeroMQ socket
    # @option opts [Fixnum] :hwm The send and receive high water mark for the ZeroMQ socket
    # @option opts [String] :identity The socket identity for router, req and sub sockets
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
    # @example Bind on loopback port 1234 with a PULL socket
    #  Hastur::Util.bind_socket(ctx, ZMQ::PULL, "tcp://127.0.0.1:1234")
    #
    # @param ctx [::ZMQ::Context] ZeroMQ Context
    # @param type The ZeroMQ socket type, like ZMQ::PULL, or a symbol like :PULL
    # @param uri [String] The ZeroMQ URI
    # @param opts [Hash] Options
    # @option opts [Fixnum] :linger The number of seconds to linger for the ZeroMQ socket
    # @option opts [Fixnum] :hwm The send and receive high water mark for the ZeroMQ socket
    # @option opts [String] :identity The socket identity for router, req and sub sockets
    #
    def bind_socket(ctx, type, uri, opts = {})
      bind_or_connect_socket(ctx, type, uri, opts.merge(:connect => false, :bind => true))
    end

    private

    #
    # Create a socket and bind or connect in one go, setting sane defaults for sockopts.
    # This is used by bind_socket and connect_socket internally.
    #
    def bind_or_connect_socket(ctx, type, uri, opts = {})
      # Prevent modifying original opts object
      opts = opts.clone

      if type.kind_of?(Symbol) || type.kind_of?(String)
        type = ZMQ.const_get(type.to_s.upcase)
      end

      socket = ctx.socket(type)

      # Linger and HWM aren't strictly necessary, but the behavior
      # they enable is what we usually expect.  For now, have all
      # sockets use the same options.  Set socket options *before*
      # bind or connect.
      opts[:linger] = 1 unless opts.has_key?(:linger)
      opts[:hwm]    = 1 unless opts.has_key?(:hwm)

      setsockopts(socket, opts)

      status = 0
      if opts[:bind]
        if uri.respond_to?(:each)
          uri.each do |one_uri|
            rc = socket.bind to_valid_zmq_uri(one_uri)
            ok = ZMQ::Util.resultcode_ok?(rc)
            hastur_logger.error "Error #{zmq_error} when binding socket to #{one_uri.inspect}!" unless ok
          end
        else
          rc = socket.bind to_valid_zmq_uri(uri)
          ok = ZMQ::Util.resultcode_ok?(rc)
          hastur_logger.error "Error #{zmq_error} when binding socket to #{uri.inspect}!" unless ok
        end
      elsif opts[:connect]
        if uri.respond_to?(:each)
          uri.each do |one_uri|
            rc = socket.connect to_valid_zmq_uri(one_uri)
            ok = ZMQ::Util.resultcode_ok?(rc)
            hastur_logger.error "Error #{zmq_error} when connecting socket to #{one_uri.inspect}!" unless ok
          end
        else
          rc = socket.connect to_valid_zmq_uri(uri)
          ok = ZMQ::Util.resultcode_ok?(rc)
          hastur_logger.error "Error #{zmq_error} when connecting socket to #{uri.inspect}!" unless ok
        end
      else
        raise "Must provide either bind or connect option to bind_or_connect_socket!"
      end
      socket
    end
  end
end
