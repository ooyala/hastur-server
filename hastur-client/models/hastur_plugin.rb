#
# Encapsulates all of the properties and characteristics of a Hastur Plugin
#
require "rubygems"
require "timeout"
require_relative "../lib/hastur_logger"
require_relative "../lib/hastur_messenger"

class HasturPlugin
  attr_accessor :name, :path

  REGISTER_PLUGIN="register_plugin"
  # TODO(viet): proably want to get this dynamically from some configuration
  TIMEOUT_BEFORE_KILL=5

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
      HasturLogger.instance.error( "Unable to initialize plugin #{plugin_json}: #{e.message}" )
    end
  end

  #
  # Runs the plugin script asychronously, once
  #
  def execute
    t = Thread.start do
      pid = nil
      begin
        # asynchronously run the plugin with a timeout
        status = Timeout::timeout( TIMEOUT_BEFORE_KILL ) do
          pipe = IO.popen(@path)
          pid = pipe.pid
          lines = pipe.readlines
          # TODO(viet): massage the raw output from the plugin before shipping it across the wire
          HasturMessenger.instance.send( "TODO(viet): #{lines}" )
        end
        # block until the pid is killed or naturally terminates
        Process.waitpid(pid)
        if $?.success?
          puts "Successfully executed plugin #{@name}"
        else
          puts "Error occurred when executing plugin #{@name}"
        end
      rescue Timeout::Error => e
        Process.kill 'TERM', pid unless pid.nil?
        puts "Unable to execute the plugin [#{@name}] within #{TIMEOUT_BEFORE_KILL} seconds"
      rescue Exception => e
        puts "Unable to execute plugin #{@name}"
      end
    end
  end

end
