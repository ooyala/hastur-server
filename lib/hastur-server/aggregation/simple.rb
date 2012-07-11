require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    extend self

    @functions.merge!({
      "sum"   => :sum,
      "diff"  => :diff,
      "min"   => :min,
      "max"   => :max,
      "first" => :first,
      "last"  => :last,
      "slice" => :slice
    })

    #
    # Remove all but the first [n=1] elements in the series.
    #
    # @param [Hash] series
    # @param [Fixnum] count default 1
    # @return [Hash] series
    #
    def first(series, count=1)
      slice(series, 0, count-1)
    end

    #
    # Remove all but the last [n=1] elements in the series.
    #
    # @param [Hash] series
    # @param [Fixnum] count default 1
    # @return [Hash] series
    #
    def last(series, count=1)
      slice(series, count * -1, -1)
    end

    #
    # Take a fixed slice of elements from the series.
    #
    # @param [Hash] series
    # @param [Fixnum] first_idx the starting index
    # @param [Fixnum] last_idx the final index
    # @return [Hash] series
    #
    def slice(series, first_idx, last_idx)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, ts_val|
          new_series[uuid][name] = {}
          keys = ts_val.keys[first_idx..last_idx]
          keys.each do |ts|
            new_series[uuid][name][ts] = ts_val[ts]
          end
        end
      end
      new_series
    end

    #
    # Replace the series with the running sum for each timestamp.
    #
    # @param [Hash] series
    # @param [Fixnum] first_idx the starting index
    # @param [Fixnum] last_idx the final index
    # @return [Hash] series
    # @option seed :first peek at the first value in the series for the first subtraction
    # @option seed :shift pop the first value in the series for the first subtraction
    # @option seed Numeric use the given number for the first subraction
    # @example
    #   sum() - start with 0 by default
    #   sum(shift) - shift the first value to initialize, series will be 1 item smaller
    #   sum(100) - start with an arbitrary number
    #   last(sum()) - get the final (total) value of the summed series
    #
    def sum(series, seed=0)
      map_over series, seed do |val,total|
        [val + total, val + total]
      end
    end

    #
    # Replace the series with the difference between neighboring values.
    # Useful for handling counters that are stored as absolutes.
    #
    # @param [Hash] series
    # @param [Symbol,Numeric] seed optional how to seed the difference
    # @option seed :first peek at the first value in the series for the first subtraction
    # @option seed :shift pop the first value in the series for the first subtraction
    # @option seed Numeric use the given number for the first subraction
    # @return [Hash] series
    #
    def diff(series, seed=:first)
      map_over series, seed do |val,previous|
        [previous - val, val]
      end
    end

    #
    # Find the highest value in each series and remove all other ts/vals.
    #
    # @param [Hash] series
    # @return [Hash] series
    #
    def max(series, ignore=nil)
      maxproc = proc do |val,max|
        if val > max
          [val, val]
        else
          [max, max]
        end
      end
      last(map_over(series, :first, &maxproc))
    end

    #
    # Find the lowest value in each series and remove all other ts/vals.
    #
    # @param [Hash] series
    # @return [Hash] series
    #
    def min(series, ignore=nil)
      minproc = proc do |val,min|
        if val < min
          [val, val]
        else
          [min, min]
        end
      end
      last(map_over(series, :first, &minproc))
    end

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
