#
# Encapsulates all of the properties and characteristics of a Hastur Plugin
#
require "rubygems"

class HasturService
  attr_accessor :name, :plugin, :lifetime

  REGISTER_SERVICE="register_service"
 
  #
  # Builds the HasturService from the register service JSON req
  #
  def initialize(service_json)
    begin
      plugin_info = JSON.parse(service_json)
      if plugin_info["method"] == REGISTER_SERVICE
        @name = plugin_info["params"]["name"]
        @path = plugin_info["params"]["path"]
      end
    rescue Exception => e
      STDERR.puts e.message
    end
  end

  #
  # Runs the plugin script asychronously, once
  #
  # TODO(viet): Kill the process after a certain period of time.
  #
  def execute
    t = Thread.new do
      `ruby #{@path}`
    end
  end

end
