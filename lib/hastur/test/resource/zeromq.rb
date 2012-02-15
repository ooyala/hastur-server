require 'hastur/util'
require 'hastur/test/resource/base'
require 'ffi-rzmq'

module Hastur
  module Test
    module Resource
      class ZeroMQ < Hastur::Test::Resource::Base
        attr_reader :ctx, :uri, :backend, :type, :error_count

        def initialize(opts, &block)
          @mutex = Mutex.new

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

          uri_param(opts[:bind],    @type, :bind)    if opts[:bind]
          uri_param(opts[:connect], @type, :connect) if opts[:connect]

          super(opts, &block)
        end

        # must come after @type is set
        def uri_param(val, type, method)
          case val
            when :gen
              @uri = "ipc://#{::Process.pid}-#{Hastur::Util.next_seq}"
            when :tap
              @uri = "ipc://#{::Process.pid}-#{Hastur::Util.next_seq}"
              @real_uri = "ipc://#{::Process.pid}-#{Hastur::Util.next_seq}"
              tap(@uri, @real_uri, type, method)
            when String
              @uri = val
            else
              raise ArgumentError.new "Invalid URI specifier: (#{val.class}) '#{val}'"
          end
        end

        def stop
          @running = false
        end

        def to_s
          @uri
        end

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
