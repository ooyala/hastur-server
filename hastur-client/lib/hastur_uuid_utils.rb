require "singleton"
require "uuid"

module Hastur
  module Client
    class UuidUtils
      include Singleton

      attr_accessor :uuid

      def initialize 
        @@uuid = nil
      end

      #
      # Saves the Hastur client UUID in the current location under .hastur_client_uuid
      #
      def save_uuid( uuid, filepath )
        unless File.exists?( filepath )
          # create the file with the uuid
          File.open(filepath, 'w') {|f| f.write( uuid ) }
        end
      end

      #
      # Retrieves the UUID from the current location under .hastur_client_uuid
      # if the file exists. Otherwise return a newly generated UUID.
      #
      def get_uuid
        return @@uuid unless @@uuid.nil?
        # TODO(viet): figure out how to better deal with the UUID
        filepath = "#{File.dirname(__FILE__)}/../.hastur_client_uuid"
        uuid = nil
        if File.exists?( filepath )
          # read from file to get the UUID
          f = File.new( filepath, "r")
          @@uuid = f.gets.chomp
        else
          # generate a new UUID and save it
          @@uuid = UUID.new.generate
          save_uuid( uuid, filepath )
        end
        @@uuid
      end
    end
  end
end
