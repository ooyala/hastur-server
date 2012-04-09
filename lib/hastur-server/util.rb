
module Hastur
  module Util
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
    def self.timestamp(ts=Time.now)
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
    def self.uptime(time=Time.now)
      now = timestamp(time)
      time - BOOT_TIME
    end

    #
    # keep a single, global counter for the :sequence field
    #
    @counter = 0
    def self.next_seq
      @counter+=1
    end

    UUID_RE = /\A[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z/i

    def self.valid_uuid?(uuid)
      if UUID_RE.match(uuid)
        true
      else
        false
      end
    end

    # not really thorough yet
    def self.valid_zmq_uri?(uri)
      case uri
        when %r{ipc://.};         true
        when %r{tcp://[^:]+:\d+}; true
        else;                     false
      end
    end

    #
    # return a useful socket identity, falling back to the memory address of the socket
    # if it no identity was assigned with setsockopt(ZMQ::IDENTITY, "").
    #
    def self.sockid(socket)
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

    def self.setsockopts(sock)
      rc = sock.setsockopt(::ZMQ::LINGER, -1)
      raise "Error setting ZMQ::LINGER: #{::ZMQ::Util.error_string}" unless rc > -1
      rc = sock.setsockopt(::ZMQ::HWM, 1)
      raise "Error setting ZMQ::HWM: #{::ZMQ::Util.error_string}" unless rc > -1
    end

    def self.bind(sock, uri)
      rc = sock.bind(uri)
      raise "Could not bind socket to URI '#{uri}': #{::ZMQ::Util.error_string}" unless rc > -1
    end

  end
end

