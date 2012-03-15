# Hastur-specific exceptions

module Hastur
  # thrown when one of the various packet handlers encounters problems decoding network packets
  class PacketDecodingError < StandardError
  end

  class UnsupportedError < StandardError
  end

  # thrown when something is an outright bug
  class BugError < StandardError
  end

  class ZMQError < StandardError
  end
end

