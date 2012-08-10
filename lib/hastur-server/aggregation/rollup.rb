require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    extend self
    @functions.merge!({ "rollup" => :rollup, "bin" => :bin })

    #
    # Generate a rollup for each series. Given a true option, the rollup is appended to the series
    # rather than replacing it.
    #
    # @param [String] delivery how to deliver the series
    #   "merge" means merge the rollup into the time series hash
    #   "replace" will replace the time series with the rollup data by itself
    #   default: add a new "$name.rollup" for each series

    # @example
    #   /api/name/foo.*.times_called/value?fun=rollup()
    #   /api/name/linux.proc.stat/value?fun=rollup(derivative(compound(cpu)))
    #   /api/name/linux.proc.stat/value?fun=rollup(merge,derivative(compound(cpu)))
    #   /api/name/linux.proc.stat/value?fun=rollup(replace,derivative(compound(cpu)))
    #
    def rollup(series, control, delivery="series")
      interval = control[:end_ts] - control[:start_ts] rescue nil

      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, subseries|
          new_series[uuid][name] = subseries
          unless skip_name?(control, name)
            if subseries.count > 0
              rollup = compute_rollups subseries.keys, subseries.values, interval
            else
              rollup = { :count => 0, :status => "no samples available to roll up" }
            end

            case delivery
            when "merge"
              new_series[uuid][name] = subseries.merge rollup
            when "replace"
              new_series[uuid][name] = rollup
            else
              new_series[uuid]["#{name}.rollup"] = rollup
            end
          end
        end
      end
      return new_series, control
    end

    #
    # Put the values into bins and roll the bins up.
    #
    # @param [Fixnum] bin_count how many bins use
    #
    # @example
    #   /api/name/foo.bar/value?fun=bin(8)
    #   /api/name/foo.bar/value?fun=compound(:stddev,bin(8))
    #
    def bin(series, control, bin_count=10, *args)
      each_subseries_in series, control do |name, subseries|
        new_subseries = {}

        # rely on request timestamps provided in control - especially with counters,
        # there will be variable numbers of samples available so ranges will be inconsistent
        min_ts = control[:start_ts]
        max_ts = control[:end_ts]

        # compute the bin size in microseconds
        range = max_ts - min_ts
        bin_usecs = (range / bin_count).floor

        # initialize the bins - all bins must exist in output
        0.upto(bin_count-1).map do |bin|
          key = min_ts +  bin * bin_usecs
          new_subseries[key] = { :timestamps => [], :values => [] }
        end

        # move the individual entries into bins ready for rollups
        bin_ts = min_ts
        subseries.keys.sort.each do |ts|
          # advance to the next bin if necessary
          until ts.between?(bin_ts, bin_ts + bin_usecs - 1) do
            bin_ts = bin_ts + bin_usecs
          end

          # compute_rollups requires two arrays, timestamps & values
          new_subseries[bin_ts][:timestamps] << ts
          new_subseries[bin_ts][:values] << subseries[ts]
        end

        # now use the rollup function to generate all of the useful aggregations
        new_subseries.keys.each do |bin_ts|
          new_subseries[bin_ts] = compute_rollups(
            new_subseries[bin_ts][:timestamps],
            new_subseries[bin_ts][:values],
            bin_usecs, bin_ts, (bin_ts + bin_usecs)
          )
        end

        new_subseries
      end
    end

    #
    # Given a set of timestamps & values, compute some useful rollups. Not
    # all of the computed values will be useful for all types of data, but they
    # all run fast enough that we always compute and let the user sort out what's useful.
    #
    # also used in cassandra/rollup.rb
    #
    def compute_rollups(timestamps, values, interval=nil, first_ts=nil, last_ts=nil)
      # both lists need to be in order, but the time/value relationship is not important
      timestamps.sort!
      values.sort!

      first_ts ||= timestamps.first
      last_ts  ||= timestamps.last
      elapsed  = ((last_ts || 0) - (first_ts || 0))

      rollup = {
        :min        => values[0],
        :max        => values[-1],
        :range      => (values[-1] - values[0] rescue 0),
        :sum        => (values.reduce(:+) rescue 0),
        :count      => values.count,
        :first_ts   => first_ts,
        :last_ts    => last_ts,
        :elapsed    => elapsed,
        :interval   => interval,
      }

      # compute the variance & standard deviation
      if timestamps.count >= 1 and values.count >= 1
        stddev, variance, average = stddev(values.compact)
        rollup[:stddev]   = stddev
        rollup[:variance] = variance
        rollup[:average]  = average
      else
        [:stddev, :variance, :average].each { |k| rollup[k] = nil }
      end

      # null out remaining fields if there aren't enough samples to compute useful values
      unless timestamps.count >= 2 and values.count >= 2
        [:p1, :p5, :p10, :p25, :p50, :p75, :p90, :p95, :p99, :period, :jitter].each do |p|
          rollup[p] = nil
        end
        return rollup
      end

      # http://en.wikipedia.org/wiki/Percentiles
      # median is just p50
      [1, 5, 10, 25, 50, 75, 90, 95, 99].each do |percentile|
        rank = (rollup[:count] * (percentile / 100.0) + 0.5).round
        rollup["p#{percentile}".to_sym] = values[rank]
      end

      # period standard deviation (quality)
      if timestamps.count > 1
        # convert the timestamps to a list of intervals
        last_ts = timestamps.shift
        intervals = []
        timestamps.each do |ts|
          intervals << ts - last_ts
          last_ts = ts
        end

        stddev, variance, average = stddev(intervals)
        rollup[:period] = average
        rollup[:jitter] = stddev
      end

      rollup
    end

    #
    # Simple standard deviation.
    # http://en.wikipedia.org/wiki/Standard_deviation
    #
    # @param [Array<Numeric>] list of values
    # @return [Float, Float, Float] stddev, variance, average
    #
    def stddev(list)
      return nil, nil, nil unless list.any?
      avg = list.reduce(&:+) / list.count.to_f
      dsum = list.map { |v| (v - avg) ** 2 }.reduce(&:+)
      variance = dsum / list.count.to_f
      return Math.sqrt(variance), variance, avg
    end
  end
end
