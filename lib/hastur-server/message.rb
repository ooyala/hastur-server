require 'ffi-rzmq'
require 'multi_json'
require 'hastur-server/exception'
require 'hastur-server/util'
require 'hastur-server/envelope'
require 'hastur-server/message/base'
require 'hastur-server/message/stat'
require 'hastur-server/message/event'
require 'hastur-server/message/log'
require 'hastur-server/message/ack'
require 'hastur-server/message/error'
require 'hastur-server/message/rawdata'
require 'hastur-server/message/heartbeat'
require 'hastur-server/message/plugin_exec'
require 'hastur-server/message/registration'

module Hastur
  #
  # High-level message information & handling. Most of the meat is in Hastur::Message::Base
  # and its subclasses, e.g. Hastur::Message::Error.
  #
  module Message
    CLASS_TYPE_IDS = {
      Hastur::Message::Stat         => 1,
      Hastur::Message::Event        => 2,
      Hastur::Message::Log          => 3,
      Hastur::Message::Ack          => 4,
      Hastur::Message::Error        => 5,
      Hastur::Message::Rawdata      => 6,
      Hastur::Message::Heartbeat    => 7,
      Hastur::Message::PluginExec   => 8,
      Hastur::Message::Registration => 9,
    }.freeze

    TYPE_ID_CLASSES = CLASS_TYPE_IDS.invert.freeze

    SYMBOL_CLASSES = {
      :stat         => Hastur::Message::Stat,
      :event        => Hastur::Message::Event,
      :log          => Hastur::Message::Log,
      :ack          => Hastur::Message::Ack,
      :error        => Hastur::Message::Error,
      :rawdata      => Hastur::Message::Rawdata,
      :heartbeat    => Hastur::Message::Heartbeat,
      :plugin_exec  => Hastur::Message::PluginExec,
      :registration => Hastur::Message::Registration,
    }.freeze

    CLASS_SYMBOLS = SYMBOL_CLASSES.invert.freeze

    def self.symbol?(sym)
      SYMBOL_CLASSES.has_key? sym
    end

    def self.type_id?(type_id)
      TYPE_ID_CLASSES.has_key?(type_id)
    end

    def self.symbol_to_class(sym)
      unless SYMBOL_CLASSES.has_key?(sym)
        raise BugError.new "'#{sym}' is not a valid Hastur::Message symbol."
      end
      SYMBOL_CLASSES[sym]
    end

    def self.symbol_to_type_id(sym)
      unless SYMBOL_CLASSES.has_key?(sym)
        raise BugError.new "'#{sym}' is not a valid Hastur::Message symbol."
      end
      CLASS_TYPE_IDS[SYMBOL_CLASSES[sym]]
    end

    def self.type_id_to_class(type_id)
      unless TYPE_ID_CLASSES.has_key?(type_id)
        raise BugError.new "'#{type_id}' is not a valid Hastur::Message type id."
      end
      TYPE_ID_CLASSES[type_id]
    end

    def self.type_id_to_symbol(type_id)
      unless TYPE_ID_CLASSES.has_key?(type_id)
        raise BugError.new "'#{type_id}' is not a valid Hastur::Message type id."
      end
      CLASS_SYMBOLS[TYPE_ID_CLASSES[type_id]]
    end

    #
    # receive a message from a ZeroMQ socket and return an appropriate Hastur::Message::* class,
    # 
    # object = Hastur::Message.recv(@socket)
    # object.envelope # Hastur::Envelope
    # object.payload  # usually JSON
    # object.send(@socket)
    #
    def self.recv(socket, zmq_flags=0)
      raise ArgumentError.new "First argument must be a ZMQ::Socket." unless socket.kind_of? ZMQ::Socket
      messages = []
      rc = socket.recvmsgs messages, zmq_flags
      return rc if zmq_flags != 0 and rc == -1

      raise ZMQError.new "ZMQ recvmsgs failed: '#{ZMQ::Util.error_string}'" unless rc != -1

      payload = messages[-1].copy_out_string
      messages.pop.close

      envelope = Hastur::Envelope.parse messages[-1].copy_out_string
      messages.pop.close

      klass = envelope.type_class
      klass.new :envelope => envelope, :payload => payload, :zmq_parts => messages
    end

    #
    # Like Envelope.parse but expects envelope + payload
    #
    # e.g.
    #  msg = Hastur::Message.parse(envelope, payload)
    #
    #  rc = socket.recvmsgs msgs=[]
    #  msg = Hastur::Message.parse(msgs[-2], msgs[-1])
    #  msg = Hastur::Message.parse *msgs[-2..-1]
    #
    # This is mostly intended to keep tests clean and does not do any error checking.
    #
    def self.parse(envelope_msg, payload_msg)
      envelope = Hastur::Envelope.parse envelope_msg
      klass = type_id_to_class envelope.type
      klass.new :envelope => envelope, :payload => payload_msg
    end

    #
    # Creates a new Hastur::Message from a data Hash,
    # with overrides.
    #
    # e.g.
    #  msg = Hastur::Message.from_hash(:uuid => uuid,
    #              :type => :counter, :value => 7,
    #              :labels => { "a" => "b" })
    #
    def self.from_hash(data_hash, options = {})
      # Make sure string keys work even if symbols are supplied
      options.keys.each { |k| options[k.to_s] = options[k] if k.is_a?(Symbol) }
      data_hash.keys.each { |k| data_hash[k.to_s] = data_hash[k] if k.is_a?(Symbol) }

      envelope = Hastur::Envelope.new \
        :version   => ::Hastur::Envelope::VERSION,
        :type_id   => options["type_id"] || options["type"] || data_hash["type_id"] || data_hash["type"],
        :to        => options["to"] || data_hash["to"],
        :from      => options["from"] || data_hash["from"],
        :ack       => options["ack"] || data_hash["ack"],
        :resend    => options["resend"] || data_hash["resend"],
        :sequence  => options["sequence"] || data_hash["sequence"],
        :timestamp => options["timestamp"] || data_hash["timestamp"],
        :uptime    => options["uptime"] || data_hash["uptime"],
        :hmac      => options["hmac"] || data_hash["hmac"],
        :routers   => []
    end
  end
end
