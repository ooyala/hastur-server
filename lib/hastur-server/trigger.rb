require "hastur-trigger/version"
require "hastur-trigger/state_handler"

require "hastur-trigger/pager_duty"
require "hastur-trigger/email"
require "hastur-trigger/web_hook"
require "hastur-trigger/annotation"

require "hastur-server/util"
require "hastur-server/message"
require "hastur-server/envelope"
require "hastur-server/syndicator"

# Hastur Triggers are small code snippets used to process streams of
# messages.  They are commonly used for alerting and synthetic derived
# messages like statistics or events.

module Hastur
  module Trigger
    class Context
      # Intentionally left out of filtering:
      #   - body
      #   - timestamp
      #   - plugin registration
      #   - agent registration
      # TODO(noah) - remove type when stat subtypes become message types
      # TODO(noah) - add name groups when they exist
      FILTER_BY = %w(name value type attn subject labels uuid)

      attr_accessor :state

      class << self
        attr_accessor :syndicator
        attr_accessor :on_sub_handlers
        attr_accessor :subscriptions
        attr_accessor :contexts
        attr_accessor :cassandra
        attr_accessor :logger
      end

      ::Hastur::Trigger::Context.on_sub_handlers = []
      ::Hastur::Trigger::Context.syndicator = ::Hastur::Syndicator.new
      ::Hastur::Trigger::Context.subscriptions = {}
      ::Hastur::Trigger::Context.contexts = []
      ::Hastur::Trigger::Context.cassandra = true

      def initialize(options = {})
        ::Hastur::Trigger::Context.contexts << self

        immediate_caller = caller[0]
        if immediate_caller =~ /^([^:]+):(\d+)/
          # TODO: Trim off repo root from filename, if present
          @filename = $1
        else
          raise "Can't get caller of Hastur::Trigger::Context.new from caller #{immediate_caller}!"
        end

        cass_spec = ::Hastur::Trigger::Context.cassandra
        cass_spec = [] if cass_spec == true
        if cass_spec
          @state_handler = StateHandler.new(@filename, *cass_spec)
        end
        @state = @state_handler ? @state_handler.get_state : {}

        @msg_socket = Object.new
        def @msg_socket.sendmsgs(messages)
          sub_id_msg, body_msg = messages
          receive_message(sub_id_msg.copy_out_string, body_msg.copy_out_string)
        end
      end

      def [](key)
        @state[key]
      end

      def []=(key, val)
        @state[key] = val
      end

      def contexts
        ::Hastur::Trigger::Context.contexts.dup
      end

      def self.on_subscribe(&block)
        ::Hastur::Trigger::Context.on_sub_handlers << block
      end

      private

      def subscribe_to(filter_opts)
        syndicator = Hastur::Trigger::Context.syndicator

        sub_id = syndicator.add_filter(filter_opts)

        syndicator.add_socket(@msg_socket, sub_id)

        ::Hastur::Trigger::Context.on_sub_handlers.each do |on_sub|
          on_sub.call(self, sub_id, filter_opts)
        end

        sub_id
      end

      #
      # Stream messages of the specified type with the
      # given filters.  This method requires a block,
      # which will be run on each message in turn.
      #
      # @param [String, Symbol] type The type of message (event, heartbeat, etc)
      # @param [Hash] filters Filters on events to deliver
      # @yields A block to call on each message
      #
      def message_stream(filters = {}, &block)
        raise "Filter must specify a type!" unless filters[:type]

        bad_filter_keys = filters.keys.map(&:to_s) - FILTER_BY
        unless bad_filter_keys.empty?
          raise "You're trying to filter on #{bad_filter_keys.join(", ")}!  Allowed: #{FILTER_BY.join(", ")}"
        end

        sub_id = subscribe_to(filters)
        ::Hastur::Trigger::Context.subscriptions ||= {}
        ::Hastur::Trigger::Context.subscriptions[sub_id] =
          { :filters => filters, :proc => block, :context => self }
      end

      def self.receive_message(sub_id, message)
        raise "No such subscription as #{sub_id.inspect}!" unless ::Hastur::Trigger::Context.subscriptions &&
          ::Hastur::Trigger::Context.subscriptions[sub_id]

        # Dispatch message to correct receiver
        proc = ::Hastur::Trigger::Context.subscriptions[sub_id][:proc]
        context = ::Hastur::Trigger::Context.subscriptions[sub_id][:context]
        context.instance_exec(message, &proc)

        @state_handler.set_state(@state) if @state_handler
      end

      public

      def self.message_from_firehose(sub_id, envelope, body)
        syndicator = ::Hastur::Trigger::Context.syndicator
        filter = syndicator.filter_for_id(sub_id)
        raise "No filter for subscription ID: #{sub_id}" unless filter

        message = ::Hastur::Trigger::Message.new(envelope, body)
        filter_value = message.body_hash.merge(message.envelope_hash)

        if syndicator.apply_one_filter(filter, filter_value)
          @logger.info "Valid message: #{message}" if @logger
          receive_message(sub_id, message)
        end
      end

      def counters(filters = {}, &block)
        message_stream filters.merge(:type => :counter), &block
      end

      def gauges(filters = {}, &block)
        message_stream filters.merge(:type => :gauge), &block
      end

      def marks(filters = {}, &block)
        message_stream filters.merge(:type => :mark), &block
      end

      def events(filters = {}, &block)
        message_stream filters.merge(:type => :event), &block
      end

      def process_heartbeats(filters = {}, &block)
        message_stream filters.merge(:type => :hb_process), &block
      end

      def hb_processes(filters = {}, &block)
        message_stream filters.merge(:type => :hb_process), &block
      end

      def agent_heartbeats(filters = {}, &block)
        message_stream filters.merge(:type => :hb_agent), &block
      end

      def hb_agents(filters = {}, &block)
        message_stream filters.merge(:type => :hb_agent), &block
      end

      def every(period, &block)
        raise "No block given to .every!" unless block_given?

        context = self

        Hastur.every(period) do
          context.instance_eval(&block)
        end
      end
    end

    class Message
      ENVELOPE_ATTRS = [ :version, :type_id, :to, :from, :ack, :resend,
                         :sequence, :timestamp, :uptime, :hmac, :routers ]
      attr_reader *ENVELOPE_ATTRS

      # Alias for "from"
      attr_reader :uuid

      attr_reader :body_hash
      attr_reader :envelope_hash

      def initialize(envelope, body)
        @body_hash = MultiJson.load(body)
        raise "Can't parse JSON: #{body}!" unless @body_hash

        @envelope = ::Hastur::Envelope.parse(envelope)
        @envelope_hash = {}
        ENVELOPE_ATTRS.each do |attribute|
          attr_value = @envelope.send(attribute)
          instance_variable_set("@#{attribute}", attr_value)
          @envelope_hash[attribute] = attr_value
        end

        # Convenience aliases
        @uuid = @envelope.from
        @envelope_hash[:uuid] = @envelope_hash[:from]
        @envelope_hash[:type] = @envelope_hash[:type_id]
      end

      def from_hostname
        # TODO(noah): lookup from Hastur Retrieval Service
        "#{from}.fake-domain.com"
      end

      def to_hostname
        "#{to}.fake-domain.com"
      end

      def hostname
        from_hostname
      end

      def to_hash
        {
          "envelope" => @envelope.to_hash,
          "body" => @body_hash,
        }
      end

      def to_json
        MultiJson.dump(self.to_hash)
      end

      def method_missing(*args)
        return @body_hash[args[0].to_s] if args.size == 1 && @body_hash[args[0].to_s]

        super
      end

      def respond_to?(method_name)
        return true if @body_hash.has_key?(method_name)

        super
      end
    end
  end
end
