# This is a prototype for the message bus interface for the Hastur
# monitoring system.

require "#{File.dirname(__FILE__)}/hastur-mq/version"
require "ffi-rzmq"

module HasturMq

  #
  # Base class for all types of ZMQ::<Socket>
  #
  class Socket
    attr_accessor :socket

    #
    # Internal method used to create a socket of any acceptable type
    #
    def _create_socket(type)
      ctx = ZMQ::Context.new
      ctx.socket( type )
    end

    def close()
      @socket.close unless @socket.nil?
    end
  end

  # 
  # Abstract socket that represents any type of socket that can 'receive" messages
  #
  class InSocket < Socket
    #
    # A blocking call that retrieves one message
    #
    def recv_once
      @socket.recv_string(msg = "")
      msg
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

  #
  # Publisher socket that connects to exactly one endpoint
  #
  class Publisher < Socket
    def initialize(endpoint)
      @endpoint = endpoint
      # set up the publisher socket
      @socket = _create_socket(ZMQ::PUB)
      @socket.connect(@endpoint)
    end
    
    #
    # Sends a message through a topic. The full message will be in the following format
    #    <topic>.<msg>
    #
    def send(topic, message)
      @socket.send_string("#{topic}.#{message}")
    end
  end

  #
  # Subscriber socket that can connect to multiple endpoints
  #
  class Subscriber < InSocket
    #
    # Connects and subscribes to the endpoints and topic respectively.
    #
    # Params:
    #   endpoints - an array of endpoints represented as strings
    #   topic     - a topic to listen to for all connected endpoints
    #
    def initialize(endpoints, topic)
      @endpoints = endpoints
      @topic = topic
      # set up the subscriber socket
      @socket = _create_socket(ZMQ::SUB)
      @endpoints.each do |endpoint|
        @socket.connect(endpoint)
      end
      @socket.setsockopt(ZMQ::SUBSCRIBE, @topic)
    end
  end

  #
  # ZMQ::PUSH socket that sends messages to exactly one endpoint
  #
  class Push < Socket
    def initialize(endpoint)
      @endpoint = endpoint
      # set up a push socket
      @socket = _create_socket(ZMQ::PUSH)
      @socket.connect(@endpoint)
    end

    def send(message)
      @socket.send_string(message)
    end
  end

  #
  # ZMQ::PULL socket that connects to multiple endpoints
  #
  class Pull < InSocket
    def initialize(endpoints, topic)
      @endpoints = endpoints
      # set up the pull socket
      @socket = _create_socket(ZMQ::PULL)
      @endpoints.each do |endpoint|
        @socket.connect(endpoint)
      end
      @socket.setsockopt(ZMQ::SUBSCRIBE, topic)
    end
  end

  #
  # ZMQ::DEALER socket that connects to a router. Contains an identity flag that the user should ensure that's static-ness (e.g. server reboots)
  #
  class Dealer < Socket
    def initialize(endpoint, uuid)
      @endpoint = endpoint
      # set up a dealer socket
      @socket = _create_socket(ZMQ::DEALER)
      @socket.setsockopt(ZMQ::IDENTITY, uuid)
      @socket.connect(@endpoint)
    end

    # ZMQ::Message should automatically use the identity from the socket in its envelop.
    def send(message)
      zmq_msg = ZMQ::Message.new(message)
      @socket.send( zmq_msg )
    end
  end

end
