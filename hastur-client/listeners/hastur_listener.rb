#
# Base class for an Hastur listener. The tcp and udp socket interactions are abstracted to this level.
# The business logic for what happens on each message is up to the subclasses to decide.
#

require "socket"

class HasturListener
  attr_accessor :type, :port, :socket

  #
  # Constructs the base listener and sets up the socket objects
  #
  def initialize(port, type)
    @port = port
    @type = type

    # construct the socket objects to listen on the port
    setup_sockets()
    # asynchronously deal with messages
    listen_for_messages()
  end

  #
  # Listen for messages in an asynchronous way
  #
  def listen_for_messages
    Thread.new do
      if type == :tcp
        # listen for TCP packets
        while msg = @socket.gets
          process_message(msg)
        end
      elsif type == :udp
        # listen for UDP packets
        while true
          msg, sender = @socket.recvfrom()  # default maxlength is ~64k
          process_message(msg)
        end
      end
    end
  end

  #
  # A holder so that sub-classes can override. This method should be overrideden by subclasses to process 
  # messages off of the listener.
  #
  def process_message(msg)
    raise "process_message( msg ) is not implemented. Only sub-classes of HasturPlugin can process messages."
  end

  #
  # Sets up the sockets to listen
  #
  def setup_sockets()
    if type == :tcp             # initialize TCP socket
      @socket = TCPSocket.new("localhost", @port)
    elsif type == :udp          # initialize UDP socket
      @socket = UDPSocket.new
      @socket.bind("localhost", @port)
    else
      raise "Only supported protocols are tcp and udp. You tried #{type}."
    end
  end
end
