module Hastur
  module Input
    module Statsd
      RE = %r{
        \A\s*               # start of string, any amount of whitespace 
        (?<name>[-\.\w]+)   # stat name, letters, numbers, ., _, and - are allowed
        :                   # : separator
        (?<value>[\.\d]+)   # a number, integer or floating point
        \|                  # | separator
        (?<unit>\p{Graph}+) # the unit, e.g. "c" or "ms", but could have |@\d\.\d but don't parse that yet
        \s*\Z               # any amount of whitespace, end of string
      }xn

      # Returns nil on invalid/unparsable data.
      def self.decode_packet(data)
        RE.match(data)
      end
    end
  end
end
