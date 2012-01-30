require "singleton"
require "uuid"

module Hastur
  module Client
    class UuidUtils
      UUID_FILE = "/etc/uuid"

      include Singleton

      attr_accessor :uuid

      def initialize 
        @@uuid = nil
      end

      #
      # Retrieves the UUID from the current location under .hastur_client_uuid
      # if the file exists. Otherwise return a newly generated UUID.
      #
      def get_uuid
        return @@uuid unless @@uuid.nil?

        # if the system's UUID is set in /etc/uuid (just a single 36-byte ascii UUID in a file, no syntax)
        # we'll use that, otherwise just assume and complain that this run of the client
        # is ephemeral and generate a new UUID on-the-fly
        if File.file? UUID_FILE
          uuid_in = File.read(UUID_FILE).chomp
          if uuid_in =~ /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i
            @@uuid = uuid_in
          else
            raise "Invalid UUID in #{UUID_FILE}."
          end
        else
          # generate a new UUID on the fly
          @@uuid = UUID.new.generate

          # save it if we can, either running as root or 
          if (File.exists?(UUID_FILE) and File.writeable?(UUID_FILE)) or File.writable?(File.basename(UUID_FILE))
            File.open(UUID_FILE, "w") do |file|
              file.puts @@uuid
            end
          else
            STDERR.puts "Could not persist generated system UUID. This UUID, '#{@@uuid}', is ephemeral."
          end
        end

        @@uuid
      end
    end
  end
end
