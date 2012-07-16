require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    extend self
    @functions.merge!  "rollup" => :rollup

    #
    # Generate a rollup for each series. Given a true option, the rollup is appended to the series
    # rather than replacing it.
    #
    # @param [Hash] series
    # @param [String] delivery how to deliver the series
    #   "merge" means merge the rollup into the time series hash
    #   "replace" will replace the time series with the rollup data by itself
    #   default: add a new "$name.rollup" for each series
    # @return [Hash] series
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
        new_series = { uuid => {} }
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
    # Given a set of timestamps & values, compute some useful rollups. Not
    # all of the computed values will be useful for all types of data, but they
    # all run fast enough that we always compute and let the user sort out what's useful.
    #
    # also used in cassandra/rollup.rb
    #
    def compute_rollups(timestamps, values, interval=nil)
      # both lists need to be in order, but the time/value relationship is not important
      timestamps.sort!
      values.sort!

      rollup = {
        :min        => values[0],
        :max        => values[-1],
        :range      => values[-1] - values[0],
        :sum        => values.reduce(:+),
        :count      => values.count,
        :first_ts   => timestamps[0],
        :last_ts    => timestamps[-1],
        :elapsed    => timestamps[-1] - timestamps[0],
        :interval   => interval,
      }

      # http://en.wikipedia.org/wiki/Percentiles
      # median is just p50
      last = values.count - 1
      [1, 5, 10, 25, 50, 75, 90, 95, 99].each do |percentile|
        rank = (rollup[:count] * (percentile / 100.0) + 0.5).round
        rollup["p#{percentile}".to_sym] = values[rank]
      end

      # compute the variance & standard deviation
      stddev, variance, average = stddev(values)
      rollup[:stddev]   = stddev
      rollup[:variance] = variance
      rollup[:average]  = average

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
      avg = list.reduce(&:+) / list.count.to_f
      dsum = list.map { |v| (v - avg) ** 2 }.reduce(&:+)
      variance = dsum / list.count.to_f
      return Math.sqrt(variance), variance, avg
    end
  end
end
