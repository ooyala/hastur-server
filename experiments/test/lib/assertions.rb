#
# Module used to perform more complex assertions.
#
# TODO(viet); Figure out a way to do equals on lists, single value, hashs, etc.
#
module Hastur
  module Test
    module Assert
      extend self

      #
      # Ensures that actual hashes contain the same information as the expected values.
      #
      def packet_equal(expected, actual)
        queue = [ expected ]
        actual_queue = [ actual ]
        while !queue.empty?
          e = queue.pop
          a = actual_queue.pop
          e.each do |k,v|
            if v.class == Hash
              return false if a[k].nil? || a[k].class != Hash
              queue << v
              actual_queue << a[k]
            else
              return false if a[k] != v
            end
          end
        end
        true
      end

    end
  end
end
