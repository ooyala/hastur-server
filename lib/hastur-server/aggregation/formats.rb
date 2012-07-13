require "hastur-server/aggregation/base"
require "hastur-server/time_util"

module Hastur
  module Aggregation
    extend self

    @functions.merge! "format_cubism" => :format_cubism

    def format_cubism(series, control, *args)
      out = []
      series.each do |uuid, name_series|
        name_series.each do |name, subseries|
          subseries.each do |ts, val|
            t = Hastur::TimeUtil.usec_to_time(ts)
            out << { :time => t.iso8601, :value => val }
          end
        end
      end
      return out, control
    end
  end
end
