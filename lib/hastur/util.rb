
module Hastur
  module Util
    # application boot time, intentionally not system boot time
    BOOT_TIME = Time.new.to_f

    # return the current uptime in floating point seconds
    def self.uptime(time=Time.new)
       time - BOOT_TIME
    end

    #
    # keep a single, global counter for the :sequence field
    #
    @counter = 0
    def self.next_seq
      @counter+=1
    end

    UUID_RE = /\A[a-f0-9]{8}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{4}-?[a-f0-9]{12}\Z/i

    def self.valid_uuid?(uuid)
      if UUID_RE.match(uuid)
        true
      else
        false
      end
    end

    # not really thorough yet
    def self.valid_zmq_uri?(uri)
      if uri =~ /\Aipc:\/\/./
        true
      elsif uri =~ /tcp:\/\/[^:]+:\d+/
        true
      else
        false
      end
    end
  end
end

