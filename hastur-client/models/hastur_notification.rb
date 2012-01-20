#
# Hastur Notification representation.
#

require "uuid"

module Hastur
  class Notification
    attr_accessor :name, :subsystem, :uuid, :id
    def initialize(name, subsystem, uuid, time = nil)
      @name = name
      @subsystem = subsystem
      @uuid = uuid
      @id = UUID.new.generate
      @time = time.nil? ? Time.new : time   # timestamp this notification
    end

    def to_json
      h = Hash.new
      h['name'] = @name
      h['subsystem'] = @subsystem
      h['uuid'] = @uuid
      h['id'] = @id
      h.to_json
    end
  end
end
