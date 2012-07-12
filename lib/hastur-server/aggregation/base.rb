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
    #   :first peek at the first value to initialize the state variable
    #   :shift pop the first entry in the series to init the state
    #   Numeric use the given number to init the state
    # @param [Array<Object>] *args additional arguments
    # @yield [Numeric, Numeric, ...] call the block with the current value, the state
    #        value, and any extra arguments passed into map_over.
    # @return [Hash] series
    #
    def map_over(series, seed, *args)
      each_subseries_in series do |name, subseries|
        case seed
          when :shift  ; state = subseries.delete(subseries.first.first)
          when :first  ; state = subseries[subseries.first.first]
          when Numeric ; state = seed
        end

        new_series = {}
        subseries.each do |ts,val|
          new_series[ts], state = yield val, state, *args
        end
        new_series
      end
    end

    #
    # Wrap up some boilerplate loops for rewriting one level shallower than map_over.
    #
    # @param [Hash] series
    # @yield name, subseries
    # @yieldreturn [Object] data to be placed under new_series[key][name]
    # will drop the name from the results entirely if false/nil
    #
    def each_subseries_in(series)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, subseries|
          new_series[uuid][name] = yield name, subseries
          unless new_series[uuid][name]
            new_series[uuid].delete name
          end
        end
      end
      new_series
    end
  end
end
