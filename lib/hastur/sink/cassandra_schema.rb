require "cassandra"
require "multi_json"
require "date"

module Hastur
  module Cassandra
    extend self

    # Default granularity: 5 minutes
    def row_key(json_hash, granularity = 5 * (60 * 60))
      # TODO(noah): Add type (gauge, counter, etc) when I know the JSON key for it

      # :name, :value, :timestamp
      time = Time.at(json_hash[:timestamp])

      # Timestamp of start of day
      date = time.to_date.to_time

      # How many seconds we are into the day
      secs_into_day = (time - date).to_i

      time_division = (secs_into_day / granularity) * granularity

      # This is the time, rounded down to the nearest 'granularity' seconds from start of day
      time_segment = date.to_i + time_division

      "#{json_hash[:uuid]}-#{time_segment}"
    end

    CF_FOR_STAT_TYPES = {
      :counter => :StatsCounter,
      :gauge => :StatsGauge,
      :timer => :StatsTimer,
      :mark => :StatsMark,
    }

    def column_family_for_stat_type(type)
      CF_FOR_STAT_TYPES[type]
    end

    # Options:
    #   :ttl
    #   :consistency
    def insert_stat(client, json_string, options = { :consistency => 2 })
      hash = MultiJson.decode(json_string, :symbolize_keys => true)
      key = ::Hastur::Cassandra.row_key(hash)
      cf = CF_FOR_STAT_TYPES[hash[:type].to_sym]
      raise "Unknown stat type #{hash[:type].inspect}!" unless cf

      name = hash[:name]
      value = hash[:value]
      colname = "#{name}-#{hash[:timestamp]}"

      if options.has_key?(:uuid)
        hash[:uuid] = options[:uuid]
        json_string = MultiJson.encode(hash)
      end

      client.insert(:StatsArchive, key, { colname => json_string }, options)
      client.insert(cf, key, { colname => value.to_s }, options)

    end

  end
end
