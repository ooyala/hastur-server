#
# Encapsulates all of the properties and characteristics of a Hastur Plugin
#
require "rubygems"
require "#{File.dirname(__FILE__)}/../lib/hastur_error_processor"

class HasturPlugin
  attr_accessor :name, :path

  REGISTER_PLUGIN="register_plugin"
 
  #
  # Builds the HasturPlugin from the plugin JSON data
  #
  def initialize(plugin_json)
    begin
      plugin_info = JSON.parse(plugin_json)
      if plugin_info["method"] == REGISTER_PLUGIN
        @name = plugin_info["params"]["name"]
        @path = plugin_info["params"]["path"]
      end
    rescue Exception => e
      HasturErrorProcessor.instance.log( "Unable to initialize plugin #{plugin_json}: #{e.message}" )
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
