#
# Base class for an Hastur listener. The tcp and udp socket interactions are abstracted to this level.
# The business logic for what happens on each message is up to the subclasses to decide.
#

require "socket"

class HasturListener
  attr_accessor :type, :port, :server, :current_thread

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
    @current_thread = Thread.new do
      if type == :tcp
        # listen for TCP clients
        loop do
          # for each client, listen to what they have to say and process each incoming message
          Thread.start(@server.accept) do |socket|
            begin
              STDOUT.puts "Accepted connection on #{@port}"
              while(msg = socket.gets)
                STDOUT.puts "tcp message recieved: #{msg}"
                process_message(msg)
              end
            rescue Exception => e
              STDERR.puts "Error occurred with recieving packets on #{@port}: #{e.message}"
            end
          end
        end
      elsif type == :udp
        # listen for UDP packets
        while msg = @socket.recv(65507)  # maxlength is 65507 bytes for UDP
          STDOUT.puts "udp message recieved: #{msg}"
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
    raise "process_message() is not implemented. Only sub-classes of HasturPlugin can process messages."
  end

  #
  # Sets up the sockets to listen
  #
  def setup_sockets()
    if type == :tcp             # initialize TCP server
      @server = TCPServer.new @port
    elsif type == :udp          # initialize UDP socket
      @socket = UDPSocket.new
      @socket.bind("localhost", @port)
    else
      raise "Only supported protocols are tcp and udp. You tried #{type}."
    end
  end
end
