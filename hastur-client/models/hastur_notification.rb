#
# Hastur Notification representation.
#

require "uuid"

module Hastur
  class Notification
    attr_accessor :name, :subsystem, :uuid, :id
    def initialize(name, subsystem, uuid, time = nil, id = nil)
      @name = name
      @subsystem = subsystem
      @uuid = uuid
      @id = id.nil? ? UUID.new.generate : id    # allow IDs to be passed in (motivation was for testing)
      @time = time.nil? ? Time.new : time       # timestamp this notification
    end

    def to_json
      params = Hash.new
      params['name'] = @name
      params['subsystem'] = @subsystem
      params['uuid'] = @uuid
      params['id'] = @id

      h = Hash.new
      h['params'] = params
      h['method'] = 'notification'
      h.to_json
    end
  end
end
