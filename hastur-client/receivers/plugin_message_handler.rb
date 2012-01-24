#
# Handles any incoming messages with 'method' set to execute_plugin. This handler
# will process the message by executing a script asynchronously with proper timeout caps.
#

require "rubygems"
require "json"
require "timeout"
require_relative "../lib/hastur_logger"
require_relative "../lib/hastur_messenger"

class PluginMessageHandler
  
  # TODO(viet): proably want to get this dynamically from some configuration
  TIMEOUT_BEFORE_KILL=5
  
  def self.handle(message)
    begin
      msg = JSON.parse(message)
      execute(msg['name'], msg['path'])
    rescue Exception => e
      HasturLogger.error("Unable to process message #{message}.\n#{e.message}\n#{e.backtrace}")
    end
  end

  #
  # Runs the plugin script asychronously, once
  #
  def self.execute(name, path)
    t = Thread.start do
      pid = nil
      begin
        # asynchronously run the plugin with a timeout
        status = Timeout::timeout( TIMEOUT_BEFORE_KILL ) do
          pipe = IO.popen("#{path}")
          pid = pipe.pid
          lines = pipe.readlines
          # TODO(viet): massage the raw output from the plugin before shipping it across the wire
          HasturMessenger.send( "TODO(viet): #{lines}" )
        end
        # block until the pid is killed or naturally terminates
        Process.waitpid(pid)
        if $?.success?
          HasturLogger.log "Successfully executed plugin #{name}"
        else
          HasturLogger.log "Error occurred when executing plugin #{name}"
        end
      rescue Timeout::Error => e
        Process.kill 'TERM', pid unless pid.nil?
        HasturLogger.error "Unable to execute the plugin [#{name}] within #{TIMEOUT_BEFORE_KILL} seconds"
      rescue Exception => e
        HasturLogger.error "Unable to execute plugin #{name}"
      end
    end
  end
end
