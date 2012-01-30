# Handle Hastur Client UUID's. 
#
# This is a fairly simple implementation that favors always returning a valid value
# and keeping the client running and relying on other forms of system management
# to make sure /etc/uuid is correct (e.g. puppet).

require "uuid"

module Hastur
  module Client
    module UUID
      @@system_uuid_file = "/etc/uuid" # Default location of the system's UUID

      #
      # Retreive the client UUID from /etc/uuid if it's there. If the file doesn't
      # exist or has bad data, generate a new UUID and return it. If /etc/uuid is
      # writable, write it to make the UUID persistent.
      #
      def self.get_uuid
        if File.readable? @@system_uuid_file
          begin
            uuid = self.get_uuid_from_system()
          rescue
            uuid = self.generate_uuid
          end
        else
          uuid = self.generate_uuid
        end

        uuid
      end

      #
      # Generates a new UUID and saves it on disk
      #
      def self.generate_uuid
        uuid = ::UUID.new.generate
        begin
          self.save_system_uuid(uuid)
        rescue Exception => e
          STDERR.puts e.message
        end
        uuid
      end

      #
      # Retrieves the UUID from /etc/uuid. Raises expections on errors.
      #
      def self.get_uuid_from_system
        # if the system's UUID is set in /etc/uuid (just a single 36-byte ascii UUID in a file, no syntax)
        uuid_in = File.read(@@system_uuid_file).chomp
        if uuid_in =~ /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i
          return uuid_in
        else
          raise "Invalid UUID in #{@@system_uuid_file}."
        end
      end

      #
      # Try save the system UUID if we can, either running as root or if it's an empty file that Hastur
      # happens to have write access to.
      #
      def self.save_system_uuid(uuid)
        if File.writable?(@@system_uuid_file) or File.writable?(File.dirname(@@system_uuid_file))
          File.open(UUID_FILE, "w") do |file|
            file.puts uuid
          end
        else
          raise "Could not persist generated system UUID. This UUID, '#{uuid}', is ephemeral."
        end

        uuid
      end
    end
  end
end

