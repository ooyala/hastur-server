require 'multi_json'

module Hastur
  module Input
    module JSON
      RE = /\A\s*{.*}\s*\Z/

      def self.decode_packet(data)
        MultiJson.decode(data)
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
