require 'hastur/util'
require 'hastur/exception'

module Hastur
  module Input
    module Statsd
      RE = %r{
        \A\s*             # start of string, any amount of whitespace 
        (?<name>[-\.\w]+) # stat name, letters, numbers, ., _, and - are allowed
        :                 # : separator
        (?<values>         # capture all values
          [\.\d]+         # a number, integer or floating point
          \|              # | separator
          \p{Graph}+      # the unit, e.g. "c" or "ms", but could have |@\d\.\d but don't parse that yet
        )\s*\Z
      }xn

      @counters = {}

      def self.counters
        @counters.clone
      end

      def self.decode_packet(data)
        time = Time.now # get the time before any processing, statsd times are server-side
        stat = RE.match(data)

        if stat.nil?
          raise Hastur::PacketDecodingError.new("Invalid statsd packet")
        end 

        name = stat[:name].gsub(/\s+/, '_').gsub(/\//, '-').gsub(/[^a-zA-Z_\-0-9\.]/, '');
        out = nil

        # statsd has a similar loop, but it doesn't look like it actually supports multiple values
        # separated by : so mimic for the moment and verify later
        stat[:values].split(/:/).each do |item|
          # TODO: ignoring sample_rate for now, implement it later if we need it
          value_in, unit, sample_rate = item.split(/\|/)
          value = value_in.to_i
          type = :counter

          if unit == 'c' 
            if @counters.has_key? name
              value = @counters[name] += value
            else
              value = @counters[name] = value
            end
          elsif unit == 'ms'
            type = :gauge
          end
          
          out = {
            :_route    => :stat,
            :name      => name,
            :type      => type,
            :value     => value,
            :timestamp => time.to_f * 1_000_000,
            :labels    => {
              :source      => :statsd,
              :units       => unit,
              :original    => data,
              :sample_rate => sample_rate
            }
          }

        end
        
        return out
      end

      # Returns nil on invalid/unparsable data.
      def self.decode(data)
        begin
          self.decode_packet(data)
        rescue
          nil
        end
      end
    end
  end
end
