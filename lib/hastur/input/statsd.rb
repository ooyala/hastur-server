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
    end

    def self.decode_packet(data, tolerant)
      stat = RE.match(data)
      if stat.nil? and tolerant == false
        raise "Packet did not match Statsd regular expression."
      end
      stat
    end
  end
end
