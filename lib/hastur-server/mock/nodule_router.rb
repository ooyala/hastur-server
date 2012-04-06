require 'ffi-rzmq'
require "hastur-server/message"
require "hastur-server/util"
require "nodule/zeromq"

module Hastur
  module Mock
    class NoduleRouter < Nodule::ZeroMQ
      attr_reader :dynamic

      #
      # r = Hastur::Mock::NoduleRouter.new
      #
      def initialize
        super :uri => :gen, :bind => ZMQ::ROUTER, :reader => :capture

        @dynamic = {}

        add_reader do |messages|
          e = Hastur::Envelope.parse(messages[-2])
          if e.type_symbol == :noop
            @dynamic[e.from] = messages[0]
          end
        end
      end

      def forward(msg)
        to = msg.envelope.to
        if @dynamic.has_key? to
          msg.zmq_parts = @dynamic[to]
          msg.envelope.add_router 'fafafafa-fafa-fafa-fafa-fafafafafafa'
          msg.send(self.socket)
        else
          raise "Cannot forward message to #{to}: it has not sent any noops to the router (yet)!"
        end
      end
    end
  end
end
