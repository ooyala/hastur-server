require 'hastur/util'
require 'hastur/test/resource/base'
require 'ffi-rzmq'
require 'thread'

module Hastur
  module Test
    module Resource
      #
      # A resource for setting up and testing ZeroMQ message flows. The most basic usage will provide
      # auto-generated IPC URI's, which can be handy for testing. More advanced usage uses the built-in
      # tap device to sniff messages while they're in-flight.
      #
      class ZeroMQ < Hastur::Test::Resource::Base
        attr_reader :ctx, :uri, :method, :type, :limit, :error_count

        #
        #  :uri - either :gen or a string, :gen means generate an IPC URI
        #
        def initialize(opts)
          @ctx = opts[:ctx] || ::ZMQ::Context.new
          @zmq_thread = nil
          @mutex = Mutex.new
          @error_count = 0
          @unlinks = []
          @sockprocs = []
          @limit = nil

          case opts[:uri]
            # Socket files are specified so they land in PWD, in the future we might want to specify a temp
            # dir, but that has a whole different bag of issues, so stick with simple until it's needed.
            when :gen
              file = "#{::Process.pid}-#{Hastur::Util.next_seq}"
              @unlinks << file
              @uri = "ipc://#{file}"
            when String
              @uri = val
            else
              raise ArgumentError.new "Invalid URI specifier: (#{val.class}) '#{val}'"
          end

          if opts[:connect] and opts[:bind] and opts[:connect] != opts[:bind]
            raise ArgumentError.new "ZMQ socket types must be the same when enabling :bind and :connect"
          end

          if opts[:connect]
            @type = opts[:connect]
            # defer socket creation/connect until the thread is started
            @sockprocs << proc do
              socket = @ctx.socket(@type)
              socket.connect(@uri)
              socket
            end
          end

          if opts[:bind]
            @type = opts[:bind]
            # defer socket creation/bind until the thread is started
            @sockprocs << proc do
              socket = @ctx.socket(@type)
              socket.bind(@uri)
              socket
            end
          end

          if opts[:limit]
            @limit = opts[:limit]
          end

          super(opts)
        end

        def run
          super
          @zmq_thread = Thread.new do
            begin
              # sockets have to be created inside the thread that uses them
              sockets = @sockprocs.map { |p| p.call }

              _zmq_write(sockets)
              _zmq_read(sockets)

              sockets.each do |socket|
                socket.setsockopt(ZMQ::LINGER, 0)
                socket.close
              end
            rescue
              STDERR.puts $!.inspect, $@
            end
          end
        end

        #
        # Set a mutex that causes the ZMQ thread to exit, join that thread, then call
        # any cleanup in Base.
        #
        def stop
          # I'm not entirely sure why this is sometimes getting called twice, but this
          # seems to make everything work fine for now.
          @mutex.lock unless @mutex.locked?
          @zmq_thread.join
          super
        end

        #
        # Return the URI generated/provided for this resource. For tapped devices, the "front" side
        # of the tap is returned.
        #
        def to_s
          @uri
        end

        # write to the socket(s) if writer proces are defined in @writers
        # assume it's ready by the time we get here, which seems to generally work with zeromq
        #
        # one single-part:  r.add_writer proc { "a" }
        # many single-part: r.add_writer proc { ["a", "b", "c"] }
        # one multipart:    r.add_writer proc { [["a", "b"]] }
        # many multipart:   r.add_writer proc { [["a", "b"],["c","d"]] }
        def _zmq_write(sockets)
          return if @writers.empty?
          Hastur::Test::Resource.synchronize do
            sockets.each do |socket|
              @writers.each do |writer|
                output = writer.call
   
                # returned a list
                if output.respond_to? :each
                  output.each do |item|
                    # procs can send lists of lists to achieve multi-part output
                    if item.respond_to? :map
                      messages = item.map { |i| ZMQ::Message.new i }
                      socket.sendmsgs messages # ignore errors
                      messages.each { |m| m.close }
                    # otherwise, it's just a string or something with a sane to_s
                    else
                      socket.send_string item.to_s
                    end
                  end
                # returned a single item, send it as a string
                else
                  socket.send_string output.to_s
                end
              end
            end
          end
        end

        #
        # Run a poll loop (using the zmq poller) on a 1/5 second timer, reading data
        # from the socket and calling the registered procs.
        # If :limit was set, will exit after that many messages are seen/processed.
        # Otherwise, exits on the next iteration if the mutex is locked (which is done in stop).
        #
        def _zmq_read(sockets)
          return if @readers.empty?
          return if sockets.empty?

          poller = ::ZMQ::Poller.new

          # no reader blocks, that means :ignore, don't run the poll loop
          sockets.each { |s| poller.register_readable(s) }

          # read on the socket(s) and call the registered reader blocks for every message, always using
          # multipart and converting to ruby strings to avoid ZMQ::Message cleanup issues.
          count = 0
          loop do
            break if @mutex.locked?

            rc = poller.poll(0.2)
            if rc <= 0
              sleep 0.2
              next
            end

            poller.readables.each do |sock|
              rc = sock.recv_strings messages=[]
              if rc > -1
                count += 1
                Hastur::Test::Resource.synchronize do
                  @readers.each do |a|
                    a.call(messages)
                  end
                end
                return if @limit and count == @limit
              else
                @error_count += 1
                break
              end

              break if @limit and count == @limit
            end
          end
        end
      end
    end
  end
end
