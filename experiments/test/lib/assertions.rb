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

      def packet_list_equal(expected, actual)
        # TODO(viet): Figure out something that is better than O(n^2).
        #             But for small lists, this should be sufficient
        return false if expected.size != actual.size
        e_idx = 0
        a_idx = 0
       
        # brute force, pair-wise comparison
        while e_idx < expected.size
          a_idx = 0
          while a_idx < actual.size
            if packet_equal expected[e_idx], actual[a_idx]
              expected.delete_at(e_idx)
              actual.delete_at(a_idx)
              e_idx -= 1
              break
            end
            a_idx += 1
          end
          e_idx += 1
        end
        expected.empty? && actual.empty?
      end

    end
  end
end
