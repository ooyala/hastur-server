require 'multi_json'
require 'yajl'
require 'hastur-server/exception'

MultiJson.use :yajl

module Hastur
  module Input
    module JSON
      def self.decode_packet(data)
        hash = MultiJson.load(data, :symbolize_keys => true)

        unless hash.has_key? :type
          raise Hastur::PacketDecodingError.new "missing :type key in JSON" 
        end

        # type should always be a symbol
        hash[:type] = hash[:type].to_sym

        return hash
      end

      # Returns nil on invalid/unparsable data.
      def self.decode(data)
        # do an initial test for json-ish input before calling through to the parser
        test = data.strip
        if test.start_with?('{') and test.end_with?('}')
          decode_packet(data) rescue nil
        else
          return nil
        end
      end
    end
  end
end
