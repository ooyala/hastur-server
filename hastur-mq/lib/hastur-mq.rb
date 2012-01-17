# This is a prototype for the message bus interface for the Hastur
# monitoring system.

require "hastur-mq/version"
require "zmq"

module HasturMq

  #
  # Base class for all types of ZMQ::<Socket>
  #
  class Socket
    attr_accessor :socket

    def close()
      @socket.close unless @socket.nil?
    end
  end

  #
  # Publisher socket that binds to exactly one endpoint
  #
  class Publisher < Socket
    def initialize(endpoint)
      @endpoint = endpoint
      # set up the publisher socket
      ctx = ZMQ::Context.new
      @socket = ctx.socket(ZMQ::PUB)
      @socket.bind(@endpoint)
    end
    
    #
    # Sends a message through a topic. The full message will be in the following format
    #    <topic>.<msg>
    #
    def send(topic, message)
      @socket.send("#{topic}.#{message}")
    end
  end

  #
  # Subscriber socket that can connect to multiple endpoints
  #
  class Subscriber < Socket
    #
    # Connects and subscribes to the endpoints and topic respectively.
    #
    # Params:
    #   endpoints - an array of endpoints represented as strings
    #   topic     - a topic to listen to for all connected endpoints
    #
    def initialize(endpoints, topic)
      @endpoints = endpoints
      # set up the subscriber socket
      ctx = ZMQ::Context.new
      @socket = ctx.socket(ZMQ::SUB)
      @endpoints.each do |endpoint|
        @socket.connect(endpoint)
        puts "Subscribing to #{endpoint}"
      end
      @socket.setsockopt(ZMQ::SUBSCRIBE, topic)
    end

    #
    # A blocking call that retrieves one message
    #
    def recv_once
      @socket.recv
    end

    # 
    # A non-blocking function that will yield to the block of code everytime a message is retrieved. 
    #
    def recv_async
      @recv_thread = Thread.start do
        begin
          loop do
            yield recv_once
          end
        rescue Exception => e
          puts "#{e.message}"
        end
      end
    end

    def close
      super
      @recv_thread.kill
    end
  end

=begin

  module Push
    
  end

  module Pull
    
  end
=end
end
