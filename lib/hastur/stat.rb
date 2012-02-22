require 'multi_json'

module Hastur
  #
  # General class for handling stats in Hastur. It's a lightweight API that mirrors the JSON format.
  # Since we haven't locked the JSON format yet, this is likely to change.
  #
  class Stat
    attr_reader :name
    attr_accessor :value, :timestamp, :tags

    def initialize(opts)
      raise ArgumentError.new(":name is required")      unless opts[:name]
      raise ArgumentError.new(":value is required")     unless opts[:value]
      raise ArgumentError.new(":timestamp is required") unless opts[:timestamp]

      if opts[:tags] and not opts[:tags].kind_of?(Hash)
        raise ArgumentError.new(":tags must be a hash")
      end

      @name      = opts[:name]
      @value     = opts[:value]
      @timestamp = Hastur::Util.timestamp opts[:timestamp]
      @tags      = opts[:tags]
    end

    def to_hash
      {
        :name      => @name,
        :value     => @value,
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
