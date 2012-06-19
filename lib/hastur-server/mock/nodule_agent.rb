require 'ffi-rzmq'
require "hastur-server/version"
require "hastur-server/message"
require "hastur-server/util"
require "nodule/zeromq"

module Hastur
  module Mock
    class NoduleAgent < Nodule::ZeroMQ
      #
      # a = Hastur::Mock::NoduleAgent.new
      #
      def initialize(opts_in={})
        opts = { :uri => :gen, :reader => :capture, :connect => ZMQ::DEALER }
        super opts.merge(opts_in)
        @uuid = opts_in[:uuid] || '11111111-2222-3333-4444-555555555555'
      end

      def run
        super
        noop
      end

      def noop
        rc = Hastur::Message::Noop.new(:from => @uuid).send(self.socket)
        raise "Couldn't send noop: '#{ZMQ::Util.error_string}'" unless ZMQ::Util.resultcode_ok?(rc)
      end

      def heartbeat
        rc = Hastur::Message::HB::Agent.new(
          :from => @uuid,
          :data => {
            :name           => "hastur.agent.heartbeat",
            :value          => 0,
            :timestamp      => Hastur::Util.timestamp,
            :labels         => {
              :mocked => true,
              :version => Hastur::SERVER_VERSION,
            }
          }
        ).send(self.socket)
        raise "Couldn't send heartbeat: '#{ZMQ::Util.error_string}'" unless ZMQ::Util.resultcode_ok?(rc)
      end

      def register
        rc = Hastur::Message::Reg::Agent.new(
          :from => @uuid,
          :data => {
            :source    => self.class.to_s,
            :hostname  => "mockbox",
            :ipv4      => "192.168.254.254",
            :timestamp => Hastur::Util.timestamp
          }
        ).send(self.socket)
        raise "Couldn't send restration: '#{ZMQ::Util.error_string}'" unless ZMQ::Util.resultcode_ok?(rc)
      end
    end
  end
end
