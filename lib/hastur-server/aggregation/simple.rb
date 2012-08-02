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
      "histogram"  => :histogram,
      "compact"    => :compact
    })

    #
    # Remove all but the first <count> elements in the series.
    #
    # @param [Hash] series
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
    # Cut the series down to the requested number of samples using a simple mod & drop function.
    # The time step is not guaranteed to be uniform and is not normalized.
    #
    # @example
    #   resample(100) - return 100 samples evenly distributed across the series
    #
    def resample(series, control, samples)
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

    # is this a proper histogram?
    # @example /api/name/ots.*.times_called/value?fun=histogram(10,merge(uuid))&ago=one_hour"
    # @example /api/name/ots.*.times_called/value?fun=histogram(10,avg,merge(uuid))&ago=one_hour"
    # what else besides summing makes sense?
    def histogram(series, control, buckets=10, agg="add", *args)
      puts "buckets: #{buckets} agg: #{agg} args: #{args}"
      each_subseries_in series, control do |name, subseries|
        new_subseries = {}
        bucket_counts = {}

        # rely on request timestamps provided in control - especially with counters,
        # there will be variable numbers of samples available so ranges will be inconsistent
        min_ts = control[:start_ts]
        max_ts = control[:end_ts]

        range = max_ts - min_ts
        bucket_usecs = (range / buckets).floor

        # initialize the buckets - all buckets should exist in output
        0.upto(buckets-1).map do |bucket|
          key = min_ts +  bucket * bucket_usecs
          new_subseries[key] = 0
          bucket_counts[key] = 0
        end

        bucket = min_ts
        subseries.keys.sort.each do |ts|
          # advance to the next bucket if necessary
          until ts.between?(bucket, bucket + bucket_usecs - 1) do
            bucket = bucket + bucket_usecs
          end

          new_subseries[bucket] = new_subseries[bucket] + subseries[ts]
          bucket_counts[bucket] = bucket_counts[bucket] + 1
        end

        case agg
        when "avg"
          new_subseries.keys.each do |bucket|
            if bucket_counts[bucket] > 0
              new_subseries[bucket] = new_subseries[bucket] / bucket_counts[bucket]
            end
          end
        when "cnt"
          new_subseries.keys.each do |bucket|
            new_subseries[bucket] = bucket_counts[bucket]
          end
        when "add"
        # nothing to do for "add", it's already done
        end

        new_subseries
      end
    end

    #
    # Remove non-numeric (e.g. null, strings) values from the series, or if you provide a replacement
    # value, replace those entries in the series with that value.
    #
    # @param [Hash] series
    # @param [String,Numeric,FalseClass] replace optional value to replace nil/null in the series
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
    # @param [Fixnum] first_idx the starting index
    # @param [Fixnum] last_idx the final index
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
    def scale(series, control, multiplier=1)
      map_over series, control, multiplier do |val,previous|
        [val * multiplier, val]
      end
    end

    #
    # Find the highest value in each series and remove all other ts/vals.
    #
    # @param [Hash] series
    # @return [Hash] series
    #
    def max(series, control, ignore=nil)
      each_subseries_in series, control do |name, subseries|
        { :max => subseries.values.sort.last }
      end
    end

    #
    # Find the lowest value in each series and remove all other ts/vals.
    #
    # @param [Hash] series
    # @return [Hash] series
    #
    def min(series, control, ignore=nil)
      each_subseries_in series, control do |name, subseries|
        { :min => subseries.values.sort.first }
      end
    end

    #
    # Add up all the values in each series.
    #
    # @param [Hash] series
    # @return [Hash] series
    #
    def sum(series, control, seed=0)
      itgl, control = integral series, control, seed
      each_subseries_in itgl, control do |name, subseries|
        { :sum => subseries.values.last }
      end
    end
  end
end
