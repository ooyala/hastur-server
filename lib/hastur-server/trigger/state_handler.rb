require "cassandra"
require "digest/md5"
require "multi_json"
MultiJson.use :yajl

# Note(jbhat): Since all our column names match, compressing this CF would be a huge win
module Hastur
  module Trigger
    DEFAULT_KEYSPACE  = "HasturTrigger"
    DEFAULT_SERVERS   = ["127.0.0.1:9160"]
    DEFAULT_CF        = :TriggerState
    DEFAULT_COL       = "val"

    class StateHandler
      # @example
      #   @state_handler = Hastur::Trigger::StateHandler.new("trigger_file1")
      def initialize(filename, opts = {})
        @key = StateHandler.filename_to_rowkey(filename)
        @client = opts[:client] || StateHandler.create_client(opts[:keyspace], opts[:servers])
        @cf = opts[:cf] || DEFAULT_CF
        @col = opts[:col] || DEFAULT_COL
      end

      # @example
      #   @state_handler.set_state(@context.state)
      def set_state(state, options = {})
        raise "Argument must be a Hash" unless state.is_a? Hash
        val = MultiJson.dump state
        @client.insert(@cf, @key, { @col => val }, options)
      end

      # @example
      #   @context.state = @state_handler.get_state
      def get_state(options = {})
        val = @client.get(@cf, @key, @col, options)
        val ? MultiJson.load(val) : {}
      end

      private
      def self.filename_to_rowkey(filename)
        raise "Must pass in non-nil filename for state rowkey" unless filename
        Digest::MD5.hexdigest(filename)
      end

      def self.create_client(keyspace = nil, servers = nil)
        ::Cassandra.new(keyspace || DEFAULT_KEYSPACE, [servers || DEFAULT_SERVERS].flatten)
      end
    end
  end
end
