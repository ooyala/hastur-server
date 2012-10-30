require "multi_json"
require "termite"
require "hastur/api"
require "hastur-server/time_util"

# A collection of useful functions, including for the ffi-rzmq gem.
module Hastur
  module Util
    extend self  # Allows calling as Util.blah
    #
    # Get a Termite logger (singleton) handle.
    #
    # @return [Termite::Logger] a logger with the component set to "Hastur"
    #
    def hastur_logger
      @__hastur_internal_logger ||= ::Termite::Logger.new(:component => "Hastur")
    end

    #
    # Log an exception to the logger and to hastur consistently.
    #
    # @param [Exception] e exception object
    # @param [Logger] logger object
    #
    def log_exception(e, logger, extra=nil)
      error = {
        :class     => e.class.to_s,
        :message   => "#{extra}#{extra.nil? ? '' : ' - '}#{e.inspect}",
        :backtrace => e.backtrace
      }

      Hastur.log e.inspect, error
      logger.error error.to_s, error
    end

    #
    # @see Hastur::TimeUtil.usec_epoch
    # @deprecated Use Hastur::TimeUtil.usec_epoch instead.
    #
    def timestamp(*args)
      Hastur::TimeUtil.usec_epoch *args
    end

    # application boot time in epoch microseconds, intentionally not system boot time
    BOOT_TIME = timestamp

    #
    # Return the current process's uptime in microseconds. This is the amount of time
    # that has passed since this module was loaded.
    #
    # @param [Time] time default Time.now
    # @return [Fixnum] current process uptime in microseconds
    #
    def uptime(time=Time.now)
      now = timestamp(time)
      time - BOOT_TIME
    end

    #
    # Keep a single, global counter for the :sequence field. Not thread safe (yet).
    #
    # @return [Fixnum] next sequence number
    #
    @counter = 0
    def next_seq
      @counter+=1
    end

    UUID_RE = /\A[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z/i

    #
    # Check if the provided UUID string is a valid 36-byte hex UUID.
    # @param [String] uuid string to be tested
    # @return [Boolean]
    #
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
    # @return [Boolean]
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
    # @param [String] uri The URI to convert
    # @return [String] converted URI
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
    # @param [ZMQ::Socket,Array<ZMQ::Socket>] socks socket(s) to configure
    # @param [Hash] opts an Options array
    # @option opts [String] :linger The socket linger option
    # @option opts [String] :hwm The socket high water mark for send and receive
    # @option opts [String] :identity The socket identity for router, req and sub sockets
    #
    def setsockopts(socks, opts = {})
      [socks].flatten.each do |sock|
        rc = sock.setsockopt(::ZMQ::LINGER, opts[:linger] || -1)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Error setting ZMQ::LINGER: #{zmq_error}" unless ok

        rc = sock.setsockopt(::ZMQ::RECONNECT_IVL, opts[:reconnect_ivl] || 100)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Error setting ZMQ::RECONNECT_IVL: #{zmq_error}" unless ok

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

    #
    # Bind to a URI or list of URI's on the given socket.
    #
    # @param [ZMQ::Socket] sock ZeroMQ socket to bind
    # @param [String,Array<String>] uris a single URI or array of URI's to bind
    #
    def bind(sock, uris)
      [uris].flatten.each do |uri|
        rc = sock.bind(uri)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Could not bind socket to URI '#{uri}': #{zmq_error}" unless ok
      end
    end

    #
    # Connect to a URI or list of URI's on the given socket.
    #
    # @param [ZMQ::Socket] sock ZeroMQ socket to connect
    # @param [String,Array<String>] uris a single URI or array of URI's to connect to
    #
    def connect(sock, uris)
      [uris].flatten.each do |uri|
        rc = sock.connect(uri)
        ok = ZMQ::Util.resultcode_ok?(rc)
        hastur_logger.error "Could not connect socket to URI '#{uri}': #{zmq_error}" unless ok
      end
    end

    #
    # Read raw ZMQ messages from the provided socket, check for errors.
    # Errors are logged, and false is returned if there are any.
    #
    # @param [ZMQ::Socket] socket ZeroMQ socket to connect
    # @return [Array<ZMQ::Message>,FalseClass] either a list of messages or just false if an error was detected.
    #
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

    #
    # Send raw ZMQ messages on the provided socket, check for errors.
    # Errors are logged, and false is returned if there are any.
    # Messages are _not_ closed after sending.
    #
    # @param [ZMQ::Socket] socket ZeroMQ socket to connect
    # @param [Array<ZMQ::Message>] message list of ZMQ::Messages to send
    # @return [Boolean] success / failure, errors are logged
    #
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
    # @param [ZMQ::Socket] socket The socket
    # @return [Array<String>,FalseClass] The array of strings from a multipart message or false for failure.
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
    # @param [ZMQ::Socket] socket the socket
    # @param [Array<String>] message the strings to send
    # @return [Boolean] whether the send succeeded
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
    # @param [String] uri the URI to check.
    # @raise [StandardError] raised on invalid URI strings
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
    # @param [ZMQ::Context] ctx ZeroMQ Context
    # @param [Fixnum] type type The ZeroMQ socket type, like ZMQ::PULL, or a symbol like :PULL
    # @param [String,Array<String>] uri The ZeroMQ URI, or an Array of same
    # @param [Hash{Symbol=>Fixnum,String}] opts Options
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
    # @param [ZMQ::Context] ctx ZeroMQ Context
    # @param [Fixnum] type The ZeroMQ socket type, like ZMQ::PULL, or a symbol like :PULL
    # @param [String,Array<String>] uri The ZeroMQ URI, or an Array of same
    # @param [Hash{Symbol=>Fixnum,String}] opts Options
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
      opts[:linger] = 1_000 unless opts.has_key?(:linger)
      opts[:hwm]    = 1     unless opts.has_key?(:hwm)

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

    # make exception strings smaller in this file
    def zmq_error
      ::ZMQ::Util.error_string
    end
  end
end
