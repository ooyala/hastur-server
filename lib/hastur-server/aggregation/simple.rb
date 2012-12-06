require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    extend self

    @functions.merge!({
      "integral"   => :integral,
      "derivative" => :derivative,
      "scale"      => :scale,
      "min"        => :min,
      "max"        => :max,
      "sum"        => :sum,
      "first"      => :first,
      "last"       => :last,
      "slice"      => :slice,
      "resample"   => :resample,
      "log"        => :log,
      "compact"    => :compact,
      "test_data"  => :test_data,
    })

    #
    # Remove all but the first <count> elements in the series.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] count default 1
    # @return [Hash] series
    #
    def first(series, control, count=1)
      slice(series, control, 0, count-1)
    end

    #
    # Remove all but the last <count> elements in the series.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] count default 1
    # @return [Hash] series
    #
    def last(series, control, count=1)
      slice(series, control, count * -1, -1)
    end

    #
    # Take a fixed slice of elements from the series.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] first_idx the starting index
    # @param [Fixnum] last_idx the final index
    # @return [Hash] series
    #
    def slice(series, control, first_idx, last_idx)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, ts_val|
          if skip_name?(control, name)
            ts_val
          else
            new_series[uuid][name] = {}
            keys = ts_val.keys[first_idx..last_idx]
            raise "Index out of range. #{first_idx}..#{last_idx} out of 0..#{ts_val.count-1}." unless keys and keys.any?
            keys.each do |ts|
              new_series[uuid][name][ts] = ts_val[ts]
            end
          end
        end
      end
      return new_series, control
    end

    #
    # Cut the series down to no more than the requested number of
    # samples using a simple mod & drop function.  The time step is
    # not guaranteed to be uniform and is not normalized.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] samples Number of samples.  Defaults to 100.
    # @return [Hash] series
    #
    # @example
    #   resample(100) - return 100 samples evenly distributed across the series
    #
    def resample(series, control, samples=100)
      each_subseries_in series, control do |name, subseries|
        if samples < subseries.count
          new_subseries = {}
          count = 0
          sample_every = (subseries.count / samples).floor

          subseries.each do |ts,val|
            if count % sample_every == 0
              new_subseries[ts] = val
            end
            count = count + 1
          end
          new_subseries
        else
          subseries
        end
      end
    end

    #
    # Get the logarithm of each value in the series. Default base 10.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Numeric] base default 10
    # @return [Hash] series
    #
    def log(series, control, base=10)
      each_subseries_in series, control do |name, subseries|
        new_subseries = {}
        subseries.each do |ts,val|
          new_subseries[ts] = Math.log val, base
        end
        new_subseries
      end
    end

    #
    # Remove non-numeric (e.g. null, strings) values from the series, or if you provide a replacement
    # value, replace those entries in the series with that value.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [String,Numeric,FalseClass] replace optional value to replace nil/null in the series
    # @return [Hash] series
    #
    def compact(series, control, replace=false)
      each_subseries_in series, control do |name, subseries|
        new_subseries = {}
        subseries.each do |ts,val|
          if Numeric === val
            new_subseries[ts] = val
          elsif replace
            new_subseries[ts] = replace
          end
        end
        new_subseries
      end
    end

    #
    # Replace the series with the running sum for each timestamp.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] seed Initial seed to add to.  Defaults to 0.
    # @return [Hash] series
    #   :first peek at the first value in the series for the first subtraction
    #   :shift pop the first value in the series for the first subtraction
    #   Numeric use the given number for the first subraction
    # @example
    #   sum() - start with 0 by default
    #   sum(shift) - shift the first value to initialize, series will be 1 item smaller
    #   sum(100) - start with an arbitrary number
    #   last(sum()) - get the final (total) value of the summed series
    #
    def integral(series, control, seed=0)
      map_over series, control, seed do |val,total|
        [val + total, val + total]
      end
    end

    #
    # Replace the series with the difference between neighboring values.
    # Useful for handling counters that are stored as absolutes.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Symbol,Numeric] seed optional how to seed the difference
    #   :first peek at the first value in the series for the first subtraction
    #   :shift pop the first value in the series for the first subtraction
    #   Numeric use the given number for the first subraction
    # @return [Hash] series
    #
    def derivative(series, control, seed="first")
      map_over series, control, seed do |val,previous|
        [val - previous, val]
      end
    end

    #
    # Multiply each value in the series by the given constant. Handy for bytes to bits,
    # sectors to bytes, or positive to negative.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @return [Hash] series
    #
    def scale(series, control, multiplier=1)
      map_over series, control, multiplier do |val,previous|
        [val * multiplier, val]
      end
    end

    #
    # Find the highest value in each series and remove all other ts/vals.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @return [Hash] series
    #
    def max(series, control, ignore=nil)
      each_subseries_in series, control do |name, subseries|
        { :max => subseries.values.max }
      end
    end

    #
    # Find the lowest value in each series and remove all other ts/vals.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @return [Hash] series
    #
    def min(series, control, ignore=nil)
      each_subseries_in series, control do |name, subseries|
        { :min => subseries.values.min }
      end
    end

    #
    # Add up all the values in each series.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] seed An optional seed to add to.  Defaults to 0.
    # @return [Hash] series
    #
    def sum(series, control, seed=0)
      itgl, control = integral series, control, seed
      each_subseries_in itgl, control do |name, subseries|
        { :sum => subseries.values.last }
      end
    end

    #
    # Return a test series of increasing integers.
    #
    # @param [Hash] series
    # @param [Hash] control Control data for the series.
    # @param [Fixnum] length The length of the series.  Defaults to 10.
    # @return [Hash] series
    #
    def test_data(series, control, length=10)
      # Data goes in 1000 usec intervals up to right now
      nowish_ts = Hastur::TimeUtil.usec_epoch() - (length * 1000)

      series["test_uuid"] = { "test_stat" => {} }
      (1..length).each do |i|
        series["test_uuid"]["test_stat"][nowish_ts + 1000 * i] = i
      end

      series
    end
  end
end
