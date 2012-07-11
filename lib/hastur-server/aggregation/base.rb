module Hastur
  module Aggregation
    extend self

    @functions ||= {}

    #
    # Map over a series and yield the given proc for each entry, tracking
    # state using the tuple returned by the proc.
    #
    # @param [Hash] series
    # @param [Symbol,Numeric] seed how to seed the difference
    # @option seed :first peek at the first value to initialize the state variable
    # @option seed :shift pop the first entry in the series to init the state
    # @option seed Numeric use the given number to init the state
    # @param [Array<Object>] *args additional arguments
    # @yield [Numeric, Numeric, ...] call the block with the current value, the state
    #        value, and any extra arguments passed into map_over.
    # @return [Hash] series
    #
    def map_over(series, seed, *args)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, series|
          new_series[uuid][name] = {}

          case seed
            when :shift  ; state = series.delete(series.keys.first)
            when :first  ; state = series[series.keys.first]
            when Numeric ; state = seed
          end

          series.each do |ts,val|
            new_series[uuid][name][ts], state = yield val, state, *args
          end
        end
      end
      new_series
    end
  end
end
