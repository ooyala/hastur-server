require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    extend self

    @functions.merge! "merge" => :merge

    def merge(series, field)
      if field == "uuid"
        merge_uuids(series)
      elsif field == "name"
        merge_names(series)
      else
        raise ArgumentError.new "invalid merge argument: #{field.inspect}"
      end
    end

    def merge_uuids(series)
      new_series = { "" => {} }
      series.each do |uuid, name_series|
        name_series.each do |name, series|
          new_series[""][name] = {}
          series.each do |ts,val|
            # add 1 usec until collision is past
            while new_series[""][name].has_key? ts
              ts = ts + 1
            end
            new_series[""][name][ts] = val
          end
        end
      end
      new_series
    end

    def merge_names(series)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = { "" => {} }
        name_series.each do |name, series|
          series.each do |ts,val|
            # add 1 usec until collision is past
            while new_series[uuid][""].has_key? ts
              ts = ts + 1
            end
            new_series[uuid][""][ts] = val
          end
        end
      end
      new_series
    end
  end
end
