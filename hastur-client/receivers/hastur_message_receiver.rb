#
# A receiver that will listen to messages from sockets that this client knows of.
# The delegation of the work will is also done here.
#

require "rubygems"
require "ffi-rzmq"

require_relative "../lib/hastur_logger"
require_relative "plugin_message_handler"
require_relative "notification_ack_message_handler"

class HasturMessageReceiver 
  
  attr_accessor :socket, :recv_thread
  
  def initialize(socket)
    @socket = socket
  end

  #
  # Starts a thread that will receive messages from a socket periodically.
  #
  def start
    HasturLogger.instance.log("Attempting to start the client receiver.")
    if @recv_thread.nil?
      @recv_thread = Thread.start do
        begin
          poller = ZMQ::Poller.new
          array = []
          @socket.getsockopt(ZMQ::IDENTITY, array)
          puts "Socket identity: #{array[0]}"
          poller.register(@socket, ZMQ::POLLIN)
          loop do
            poller.poll(1)
            poller.readables.each do |s|
              messages = []
              loop do
                s.recv_string(msg = "")
                messages << msg
                has_more = s.more_parts?
                break unless has_more
              end

              # TODO(viet): do something smart with these messages
              if messages.size == 2
                if messages[0] == "execute_plugin"
                  PluginMessageHandler.handle(messages[1])
                elsif messages[0] == "notification_ack"
                  NotificationAckMessageHandler.handle(messages[1])
                end
              end
            end
          end
        rescue Exception => e
          HasturLogger.instance.error("Error occurred when receiving MQ messages: #{e.message}\n\n#{e.backtrace}")
        end
      end
    else
      raise "The receiver thread is already started."
    end
  end
end
