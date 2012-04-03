require 'ffi-rzmq'
require 'yajl'
require 'multi_json'
require 'uuid'
require 'socket'
require 'termite'

require "hastur"
require "hastur-server/message"
require "hastur-server/util"

module Hastur
  class Syndicator
    NAME_RE = %r{\A[-\.\w]+\Z}
    attr_reader :messages_processed, :messages_forwarded, :messages_dropped

    #
    # Create a new syndicator.
    # @example syndicator = Hastur::Syndicator.new
    #
    def initialize
      @filters                = [] # array of filter hashes
      @sockets                = {}
      @logger                 = Termite::Logger.new
      @messages_processed     = 0
      @messages_forwarded     = 0
      @messages_dropped       = 0
    end

    #
    # Return a _copy_ of the list of filters currently registered.
    # @return [Array<Hash>]
    #
    def filters
      # make it difficult to mess with the list directly
      @filters.map(&:dup)
    end

    #
    # Create a filter rule.  Messages are forwarded when all of the filter elements match.
    #
    # @param [Hash{Symbol => String}] opts
    # @option opts [String] :uuid
    # @option opts [String] :type
    # @option opts [String] :name
    # @option opts [String] :value
    # @option opts [String] :attn
    # @option opts [String] :subject
    # @option opts [String] :labels
    # @return [String] filter ID (uuid)
    # @example F.add_filter { :uuid => uuid }
    #
    def add_filter(opts)
      filter = {}
      id = UUID.generate

      if opts[:uuid] and Hastur::Util.valid_uuid?(opts[:uuid])
        filter[:uuid] = opts[:uuid]
      else
        raise ArgumentError.new ":uuid must be a valid 36-byte hex UUID (#{opts[:uuid]})"
      end

      if opts[:type]
        if Hastur::Message.type_id? opts[:type]
          filter[:type] = opts[:type]
        elsif Hastur::Message.symbol? opts[:type]
          filter[:type] = Hastur::Message.symbol_to_type_id(opts[:type])
        else
          raise ArgumentError.new ":type must be a valid Hastur::Message type"
        end
      end

      if opts[:name]
        # only accept strings that start with word characters, specifically trying to avoid
        # people passing in regexps or anything that doesn't make sense
        if NAME_RE.match opts[:name]
          filter[:name] = opts[:name]
        else
          raise ArgumentError.new ":name must be a string and conform to #{NAME_RE}"
        end
      end

      if opts[:attn] or opts[:subject]
        if opts[:type] and opts[:type] != Hastur::Message::Event.type_id
          raise ArgumentError.new ":attn only works for events"
        end
        filter[:type] = Hastur::Message::Event.type_id
      end

      if opts[:attn]
        if opts[:attn].kind_of? Array
          filter[:attn] = opts[:attn].map do |attn|
            raise ArgumentError.new ":attn items must be strings" unless attn.kind_of? String
            attn
          end
        else
          raise ArgumentError.new ":attn must be an array"
        end
      end

      if opts[:subject]
        unless opts[:subject].kind_of? String
          raise ArgumentError.new ":subject filter must be a string"
        end
        filter[:subject] = opts[:subject]
      end

      if opts[:labels]
        if opts[:labels].kind_of? Hash
          filter[:labels] = {}
          opts[:labels].each do |key,value|
            # auto-convert symbols to strings
            key = key.to_s if key.kind_of? Symbol
            value = value.to_s if value.kind_of? Symbol

            unless key.kind_of? String
              raise ArgumentError.new "label keys must be strings"
            end
            unless [String, Numeric, TrueValue, FalseValue].include?(value.class)
              raise ArgumentError.new "label values must be string/numeric/true/false"
            end

            filter[:labels][key] = value
          end
        else
          raise ArgumentError.new ":labels must be an array"
        end
      end

      @sockets[id] = []
      @filters << [ id, filter ]

      id
    end

    #
    # add a destination socket to a filter by ID
    # @param [ZMQ::Socket] socket
    # @param [String] filter ID (36 byte UUID)
    #
    # The only method called on sockets in this class is sendmsgs, so if you
    # want to mock a socket, that's all you need. It will be given two
    # ZMQ::Message objects that you should probably copy_out_string.
    #
    def add_socket(socket, filter_id)
      raise ArgumentError.new "First arg must respond to :sendmsgs." unless subsock.respond_to? :sendmsgs
      unless Hastur::Util.valid_uuid? filter_id
        raise ArgumentError.new "Second arg must be a 36-byte filter ID (uuid)."
      end

      @sockets[filter_id] << socket
    end

    #
    # run exactly one filter
    # @param [Hash] filter
    # @param [Hash] message
    # @yield [Hash] call the block with the message if the filter passes
    #
    def apply_one_filter(filter, message)
      do_forward = true
      labels_matched = false

      filter.each do |key, value|
        mkey = nil

        # message keys may not be symbols, check for both sym & str
        if message.has_key?(key)
          mkey = key
        # convert to string
        elsif message.has_key?(key.to_s)
          mkey = key.to_s
        else
          # message does not have the key in either string or symbol form
          if mkey.nil?
            # the only way to proceed is if the filter's value is false
            unless filter[key] == false
              do_forward = false
              next
            end
          # the message has the key
          else
            # success unless the filter says the key should not be there with value of false
            if filter[key] == false
              do_forward = false
              next
            end
          end
        end

        # filter requires the key is present, value is ignored
        if filter[key] == true and mkey.nil?
          do_forward = false
        # filter requires the key is NOT present, value is ignored
        elsif filter[key] == false and not mkey.nil?
          do_forward = false
        # filter requires the key is present and the values match exactly
        elsif message[mkey] != filter[key]
          do_forward = false
        end
      end

      return unless do_forward

      # process labels separately, using a recursive call, it should only ever be one level
      # unhandled (stupid) case: user specifies a >= 1 filter labels all set to false
      if filter[:labels]
        if message.has_key?(:labels)
          lkey = :labels
        elsif message.has_key?("labels")
          lkey = "labels"
        else
          raise "BUG! Should not have gotten to this point if message has no labels but the filter requires them."
        end

        apply_one_filter filter[:labels], message[lkey] do |m|
          labels_matched = true
        end
      end

      if do_forward and labels_matched
        yield message
      end
    end

    #
    # Run all of the registered filters on a message.
    # @param [Hash] message a message in hash format
    # @yield [Hash] block to call if there is an error sending on the socket
    # @return [Fixnum] number of times the message was forwarded
    #
    def apply_all_filters(message)
      times_forwarded = 0
      @messages_processed += 1

      @filters.each do |id, filter|
        apply_one_filter message, filter do
          @sockets[id].each do |socket|
            out = [
              ZMQ::Message.new(id),
              ZMQ::Message.new(message)
            ]

            rc = socket.sendmsgs out
            if ZMQ::Util.resultcode_ok?(rc)
              times_forwarded += 1
            else
              errdata = { :rc => rc, :id => id, :message => message, :filter => filter.dup, :socket => socket }
              yield errdata
            end

            out.each do |m| m.close end
          end
        end
      end

      @messages_forwarded += times_forwarded
      if times_forwarded == 0
        @messages_dropped += 1
      end

      times_forwarded
    end
  end
end
