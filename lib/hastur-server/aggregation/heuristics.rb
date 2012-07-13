require "hastur-server/aggregation/base"

# http://en.wikipedia.org/wiki/Heuristic#Computer_science
#
# In computer science, a heuristic is a technique designed to solve a problem that ignores whether
# the solution can be proven to be correct, but which usually produces a good solution or solves
# a simpler problem that contains or intersects with the solution of the more complex problem.
#

module Hastur
  module Aggregation
    extend self

    @functions.merge! "unrollover" => :unrollover

    #
    # Detect rollovers and make it as if they never happened.
    # This assumes it has been given a counter and that you're OK with getting
    # possibly huge numbers. It also assumes that it's an increasing counter.
    #
    def unrollover(series, ignore=nil)
      each_subseries_in series do |name, subseries|
        new_subseries = {}

        # track the previous value, once a rollover is detected, start adding it to new values
        prev = subseries.first.last
        last_in_previous_series = nil

        # sort the timestamps again just to be sure, otherwise things will get very weird
        subseries.keys.sort.each do |ts|
          val = subseries[ts]

          if prev > val
            last_in_previous_series = prev
          end

          if last_in_previous_series
            new_subseries[ts] = val + last_in_previous_series
          else
            new_subseries[ts] = val
          end

          prev = val
        end
        new_subseries
      end
    end
  end
end

