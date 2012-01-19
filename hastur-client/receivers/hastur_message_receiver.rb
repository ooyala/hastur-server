require "rubygems"
require "ffi-rzmq"

class HasturMessageReceiver 
  
  attr_accessor :socket, :recv_thread
  
  def initialize(socket)
    @socket = socket
  end

  def start
    if @recv_thread.nil?
      @recv_thread = Thread.start do
        begin
          poller = ZMQ::Poller.new
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
              messages.each do |m|
                puts "recv => #{m}"
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
