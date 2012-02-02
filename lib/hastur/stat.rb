require 'multi_json'

module Hastur
  #
  # General class for handling stats in Hastur. It's a lightweight API that mirrors the JSON format.
  # Since we haven't locked the JSON format yet, this is likely to change.
  #
  class Stat
    attr_reader :name
    attr_accessor :value, :units, :timestamp, :tags

    def initialize(opts)
      raise ArgumentError.new(":name is required")      unless opts[:name]
      raise ArgumentError.new(":value is required")     unless opts[:value]
      raise ArgumentError.new(":units is required")     unless opts[:units].kind_of?(String)

      # automatically convert Time and Fixnum to a Float
      if opts[:timestamp].kind_of?(Time) or opts[:timestamp].kind_of?(Fixnum)
        opts[:timestamp] = opts[:timestamp].to_f
      end

      unless opts[:timestamp].kind_of?(Float)
        raise ArgumentError.new(":timestamp is required")
      end

      if opts[:tags] and not opts[:tags].kind_of?(Hash)
        raise ArgumentError.new(":tags must be a hash")
      end

      @name      = opts[:name]
      @value     = opts[:value]
      @units     = opts[:units]
      @timestamp = opts[:timestamp]
      @tags      = opts[:tags]
    end

    def to_hash
      {
        :name      => @name,
        :value     => @value,
        :units     => @units,
        :timestamp => @timestamp,
        :tags      => @tags.to_hash
      }
    end

    def to_json
      MultiJson.encode to_hash()
    end

    def to_s
      to_json()
    end
  end
end
