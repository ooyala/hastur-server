require 'multi_json'
require 'yajl'
require 'hastur/exception'

MultiJson.engine = :yajl

module Hastur
  module Input
    module JSON
      RE = /\A\s*{.*}\s*\Z/

      def self.decode_packet(data)
        hash = MultiJson.decode(data, :symbolize_keys => true)

        unless hash.has_key? :method 
          raise Hastur::PacketDecodingError.new "missing :method key in JSON" 
        end

        unless hash.has_key? :params
          raise Hastur::PacketDecodingError.new "missing :params key in JSON"
        end

        return hash
      end

      # Returns nil on invalid/unparsable data.
      def self.decode(data)
        if RE.match(data)
          decode_packet(data) rescue nil
        else
          return nil
        end
      end
    end
  end
end
