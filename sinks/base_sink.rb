require "trollop"
require "hastur/message"
require "hastur/zmq_utils"
require "hastur/sink/cassandra_schema"

module Hastur
  class Sink
    def initialize
      # parse command line
      @opts = opts
      # connect to Cassandra
      puts "Connecting to database(s) at #{opts[:hosts].flatten} on part 9160"
      @client = ::Cassandra.new(@opts[:keyspace], @opts[:hosts].map {|h| "#{h}:9160" })
      @client.default_write_consistency = 2    # Initial default: 1
      # connect to Hastur router(s)
      @socket = Hastur::ZMQUtils.connect_socket(::ZMQ::Context.new, ::ZMQ::PULL, @opts[:routers].flatten)
      # properly set up signal trapping
      %w(INT TERM KILL).each do |sig|
        ::Signal.trap(sig) do
          @running = false
          ::Signal.trap(sig, "DEFAULT")
        end
      end
    end

    def start
      raise "Subclass of Hastur::Sink must override start()"
    end

    def opts
      ::Trollop::options do
        banner "Listens for ZMQ messages and allows the sink to handle it appropriately.\n\nOptions:"
        opt :hosts,    "Cassandra Hostname(s)",  :default => ["127.0.0.1"],            :type => :strings,
                                                                                       :multi => true
        opt :routers,  "Router URI(s)",          :default => ["tcp://127.0.0.1:8127"], :type => :strings,
                                                                                       :multi => true
        opt :keyspace, "Keyspace",               :default => "Hastur",                 :type => String
        opt :hwm,      "ZMQ message queue size", :default => 1,                        :type => :int
      end
    end
  end
end
