# Hastur::Input::Collectd - parse collectd UDP packets and return a hash
#
# To work on this file, you'll need the following resources handy:
#
# http://collectd.org/wiki/index.php/Binary_protocol
#
# https://github.com/octo/collectd/blob/master/src/network.h
# https://github.com/octo/collectd/blob/master/src/network.c
#
# http://ruby-doc.org/core-1.9.3/String.html#method-i-unpack
# perldoc -f pack # (the perl pack docs are more thorough)

module Hastur
  module Input
    module Collectd
      # constants from collectd/src/network.h
      TYPE_HOST            = 0x0000
      TYPE_TIME            = 0x0001
      TYPE_TIME_HR         = 0x0008
      TYPE_PLUGIN          = 0x0002
      TYPE_PLUGIN_INSTANCE = 0x0003
      TYPE_TYPE            = 0x0004
      TYPE_TYPE_INSTANCE   = 0x0005
      TYPE_VALUES          = 0x0006
      TYPE_INTERVAL        = 0x0007
      TYPE_INTERVAL_HR     = 0x0009
      TYPE_MESSAGE         = 0x0100
      TYPE_SEVERITY        = 0x0101
      TYPE_SIGN_SHA256     = 0x0200
      TYPE_ENCR_AES256     = 0x0210
      DS_TYPE_COUNTER      = 0
      DS_TYPE_GAUGE        = 1
      DS_TYPE_DERIVE       = 2
      DS_TYPE_ABSOLUTE     = 3

      # Decodes a single collectd UDP packet using offset tracking, returns a hash.
      # The first argument is a binary string (your recvfrom() buffer).
      # The second argument is a boolean. When set to true, nil is returned instead of raising
      # exceptions on invalid/unparsable packets.
      def self.decode_packet(data, tolerant)
        stats = {}
        offset = 0

        begin
          while data.length > 0
            key, value, offset = self.decode_part(data, offset)
            stats[key] = value
          end
        rescue
          if tolerant
            return nil
          else
            raise
          end
        end

        return stats
      end

      # Decodes a collectd "part" and returns key, value.
      # First argument is the packet string, the second is the current offset into the packet (FixNum bytes).
      def self.decode_part(data, offset)
        # len includes the header's 4 bytes
        # nn/a is a more technically correct unpack ... trying to track down where this is unpacking
        # bad data (invalid types/len), probably something simple I'm missing
        type, len, value = data.unpack("@#{offset}SS/a") # uint16_t, uint16_t, binary string

        case type
          when TYPE_TIME
            key = :time
            value = data.unpack('Q') # uint64_t
          when TYPE_TIME_HR
            key = :time_hr
            value = data.unpack('Q') # uint64_t
          when TYPE_INTERVAL
            key = :interval
            value = data.unpack('Q') # uint64_t
          when TYPE_INTERVAL_HR
            key = :interval_hr
            value = data.unpack('Q') # uint64_t
          when TYPE_SEVERITY
            key = :severity
            value = data.unpack('Q') # uint64_t
          when TYPE_HOST
            key = :host
            value = data.unpack("Z#{len - 4}") # ascii string
          when TYPE_PLUGIN
            key = :plugin
            value = data.unpack("Z#{len - 4}") # ascii string
          when TYPE_PLUGIN_INSTANCE
            key = :plugin_instance
            value = data.unpack("Z#{len - 4}") # ascii string
          when TYPE_TYPE
            key = :type
            value = data.unpack("Z#{len - 4}") # ascii string
          when TYPE_TYPE_INSTANCE
            key = :type_instance
            value = data.unpack("Z#{len - 4}") # ascii string
          when TYPE_MESSAGE
            key = :message
            value = data.unpack("Z#{len - 4}") # ascii string
          when TYPE_VALUES
            key = :values
            value = self.decode_values(value)
            value = data.unpack("Z#{len - 4}") # ascii string
          #when TYPE_SIGN_SHA256
          #when TYPE_ENCR_AES256
        else
          raise "Invalid packet data type: #{type}, len: #{len}."
        end

        return key, value, offset + len
      end

      # Decode a values part. These are a bit different from the other parts since they
      # contain a list of values in a slightly smaller <type><value><type><value>... format.
      def self.decode_values(data)
        values = []
        nvals, data = data.unpack("na*")

        1.upto(nvals) do |n|
          type, data = data.unpack("Ca*")

          case type
            when DS_TYPE_COUNTER
              value, data = data.unpack("Q>a*") # network (big endian) unsigned integer
              values.push value
            when DS_TYPE_GAUGE
              value, data = data.unpack("Ea*")  # x86 (little endian) double
              values.push value
            when DS_TYPE_DERIVE
              value, data = data.unpack("q>a*") # network (big endian) signed integer
              values.push value
            when DS_TYPE_ABSOLUTE
              value, data = data.unpack("Q>a*") # network (big endian) unsigned integer
              values.push value
            else
              raise "Unknown value type: #{type}"
          end
        end

        return values
      end
    end
  end
end

