require "jruby-msgpack"

class Object
  def to_msgpack
    org.msgpack.MessagePack.new.write(self)
  end
end

module MessagePack
  def self.unpack(value)
    unpacker = MessagePack::Unpacker.new
    unpacker.feed(value)
    unpacker.first
  end
end
