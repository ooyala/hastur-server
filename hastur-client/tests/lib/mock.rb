require "ffi-rzmq"

module Hastur
  module Mock
    class Router
      attr_accessor :router, :context
      @@context = nil
      def initialize
        @@context = ZMQ::Context.new if @@context.nil?
        @router = @@context.socket(ZMQ::ROUTER)
        @router.bind("tcp://127.0.0.1:8000")
      end

      def unbind
        @router.close
        @@context.terminate
        @@context = nil
      end

      def recv_multipart
        messages = []
        loop do
          @router.recv_string(msg = "")
          messages << msg
          has_more = @router.more_parts?
          break unless has_more
        end
        messages
      end
      
      def send_msg(client_id, msgs)
        @router.send_string(client_id, ZMQ::SNDMORE)

        # send all messages except for the last one
        # 0.upto( -2 ) is okay; size of messages[] is 0
        # 0.upto( -1 ) is okay; size of messages[] is 1
        # 0.upto( 0 ) should print out the first element; size of messages[] is 2
        0.upto(msgs.size-2) do |i|
          @router.send_string(msgs[i], ZMQ::SNDMORE)
        end

        @router.send_string(msgs[-1])
      end
    end
  end
end
