require "hastur-server/aggregation/base"
require "hastur-server/time_util"

module Hastur
  module Aggregation
    extend self

    @functions.merge!({
      "format" => :format,
      # deprecated, switch to format(cubism,...)
      "format_cubism" => :format_cubism
    })

    def format(series, control, which_format="none")
      case which_format
      when "cubism" ; format_cubism series, control
      when "array"  ; format_array series, control
      else ; series
      end
    end

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

    def format_array(series, control, *args)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, subseries|
          new_series[uuid][name] = []
          subseries.each do |ts, val|
            new_series[uuid][name] << { :timestamp => ts, :value => val }
          end
        end
      end
      return new_series, control
    end
  end
end
