#
# Module used to perform more complex assertions.
#
# TODO(viet); Figure out a way to do equals on lists, single value, hashs, etc.
#
module Hastur
  module Test
    module Assert
      
      #
      # Performs an assertion every second until the timeout reaches.
      # The option to "keep_timeout_alive" is used whenever the tester
      # wants to keep the equals method blocked for timeout seconds. 
      # Otherwise the equals method will return as soon as an assertion
      # is true.
      #
      def self.equal(expected, &actual, timeout=0, keep_timeout_alive=true)
        if keep_timeout_alive
          sleep timeout
          return expected == actual.call
        else
          1.upto(timeout) do
            sleep 1
            return true if expected == actual.call
          end
          return false
        end
      end
    end
  end
end
