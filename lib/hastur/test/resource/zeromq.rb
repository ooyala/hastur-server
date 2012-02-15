require 'hastur/util'
require 'hastur/test/resource/base'
require 'ffi-rzmq'

module Hastur
  module Test
    module Resource
      #
      # A resource for setting up and testing ZeroMQ message flows. The most basic usage will provide
      # auto-generated IPC URI's, which can be handy for testing. More advanced usage uses the built-in
      # tap device to sniff messages while they're in-flight.
      #
      class ZeroMQ < Hastur::Test::Resource::Base
        attr_reader :ctx, :uri, :backend, :type, :error_count

        #
        # Optional:
        #  :ctx - provide a ZeroMQ context, if unset one is created
        # Required:
        #  :type - the type of ZeroMQ socket (e.g. ZMQ::PULL)
        #  :bind / :connect - the "direction" of the socket, with a value
        #    :gen - generate an IPC URI
        #    :tap - create a tap device and run actions against messages that cross it
        #    ""   - specify a string URI (unchecked)
        #
        def initialize(opts, &block)
          @mutex = Mutex.new
          @threads = []
          @unlinks = []

          if opts[:ctx]
            @ctx = opts[:ctx]
          else
            @ctx = ::ZMQ::Context.new
          end

          if opts[:type]
            # TODO: check type is valid
            @type = opts[:type]
          else
            raise ArgumentException.new ":type is required for ZeroMQ"
          end

          _uri_param(opts[:bind],    @type, :bind)    if opts[:bind]
          _uri_param(opts[:connect], @type, :connect) if opts[:connect]

          super(opts, &block)
        end

        # must come after @type is set
        # For now, socket files are specified so they land in PWD, in the future we might want to specify a
        # temp dir, but that has a whole different bag of issues, so stick with simple until it's needed.
        def _uri_param(val, type, method)
          case val
            when :gen
              file = "#{::Process.pid}-#{Hastur::Util.next_seq}"
              @unlinks << file
              @uri = "ipc://#{file}"
            when :tap
              # generate two filenames, that become two IPC URI's
              file = "#{::Process.pid}-#{Hastur::Util.next_seq}"
              real = "#{::Process.pid}-#{Hastur::Util.next_seq}"
              @unlinks += [ file, real ]
              @uri = "ipc://#{file}"
              @real_uri = "ipc://#{real}"

              tap(@uri, @real_uri, type, method)
            when String
              @uri = val
            else
              raise ArgumentError.new "Invalid URI specifier: (#{val.class}) '#{val}'"
          end
        end

        #
        # Sets the running flag to false so the tap devices can exit cleanly. Then joins any threads
        # that are running.
        #
        def stop
          @running = false
          @threads.each { |t| t.join }

          @unlinks.each do |file|
            File.unlink(file) if File.socket?(file)
          end
        end

        #
        # Return the URI generated/provided for this resource. For tapped devices, the "front" side
        # of the tap is returned.
        #
        def to_s
          @uri
        end

        #
        # Set up a tap "device" (in zmq parlance) in a thread.  Currently, only the simple tap device is supported.
        #
        # The device calls recvmsgs, copies the data out of the message into Ruby strings (with copy_out_string),
        # calls all of the registered :action blocks inside a mutex, then forwards the messages on unmodified.
        #
        def tap(uri, backend_uri, type, method)
          backend_type = case type
            when ZMQ::PULL;   ZMQ::PUSH
            when ZMQ::PUSH;   ZMQ::PULL
            when ZMQ::PUB;    ZMQ::SUB
            when ZMQ::SUB;    ZMQ::PUB
            when ZMQ::REP;    ZMQ::REQ
            when ZMQ::REQ;    ZMQ::REP
            when ZMQ::ROUTER; ZMQ::DEALER
            when ZMQ::DEALER; ZMQ::ROUTER
            else
              raise ArgumentError.new "Unsupported or invalid ZMQ type: #{type}"
          end

          @running = true

          @thread  = Thread.new do
            begin
              backend = @ctx.socket(backend_type)
              socket  = @ctx.socket(type)

              if method == :bind
                socket.bind(uri)
                backend.connect(backend_uri)
              elsif method == :connect
                socket.connect(uri)
                backend.bind(uri)
              else
                raise "BUG: invalid method to tap(), '#{method}'"
              end

              case type
                when ZMQ::PULL, ZMQ::PUB, ZMQ::REP, ZMQ::DEALER
                  simple_tap_device(uri, backend, socket)
                when ZMQ::PUSH, ZMQ::SUB, ZMQ::REQ, ZMQ::ROUTER
                  simple_tap_device(uri, socket, backend)
              end

              backend.setsockopt(ZMQ::LINGER, 0.1)
              backend.close

              socket.setsockopt(ZMQ::LINGER, 0.1)
              socket.close
            rescue
            end
          end

          #
          # Tap & forward device.
          #
          # This device ill work for most 1:1 cases, but will cause bewildering results for 1:N or N:1 cases.
          # For now, the best bet is to keep taps out at the edge where things can generally be 1:1.  If we
          # really need to tap ROUTER/DEALER N:N scenarios, a much more sophisticated tap will have to be
          # written that creates all of the connections so the socket identities can be lined up. And even
          # then, there are weird issues for some of the use cases we have in mind.
          #
          def simple_tap_device(uri, from, to)
            @poller = ZMQ::Poller.new
            @poller.register_readable from
            @poller.register_readable to

            # TODO: figure out a decent way to hand errors back to the top
            while @running
              rc = @poller.poll(0.1)

              if rc > -1
                @poller.readables.each do |r|
                  if r == from
                    direction = :recv
                  else
                    direction = :send
                  end

                  rc = r.recvmsgs messages=[]
                  items = messages.map { |msg| msg.copy_out_string }
                  if rc > -1
                    @mutex.synchronize do
                      @actions.each do |a|
                        a.call({:messages => items, :direction => direction})
                      end
                    end
                  else
                    @error_count += 1
                  end

                  if r == from
                    rc = to.sendmsgs messages
                  else
                    rc = from.sendmsgs messages
                  end

                  @error_count +=1 unless rc > -1
                  messages.each { |msg| msg.close if msg.respond_to? :close }

                end
              else
                @error_count += 1
              end
            end
          end
        end
      end
    end
  end
end
