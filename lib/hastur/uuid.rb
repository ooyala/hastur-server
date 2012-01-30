UUID_FILE="/etc/uuid"       # Location of the system's UUID if it exists

#
# Retrieves the UUID from the system if it already exists.
#
def get_uuid_from_system
  # if the system's UUID is set in /etc/uuid (just a single 36-byte ascii UUID in a file, no syntax)
  # we'll use that, otherwise just assume and complain that this run of the client
  # is ephemeral and generate a new UUID on-the-fly
  uuid = nil
  if File.file? UUID_FILE
    uuid_in = File.read(UUID_FILE).chomp
    if uuid_in =~ /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i
      uuid = uuid_in
    else
      raise "Invalid UUID in #{UUID_FILE}."
    end
  else
    # generate a new UUID on the fly
    uuid = UUID.new.generate
    STDERR.puts "Generated new UUID: #{uuid}"
    # save it if we can, either running as root or 
    if (File.exists?(UUID_FILE) and File.writeable?(UUID_FILE)) or File.writable?(File.basename(UUID_FILE))
      File.open(UUID_FILE, "w") do |file|
        file.puts uuid
      end
    else
      STDERR.puts "Could not persist generated system UUID. This UUID, '#{uuid}', is ephemeral."
    end
  end
  uuid
end

