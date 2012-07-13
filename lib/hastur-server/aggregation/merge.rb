require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    extend self

    @functions.merge! "merge" => :merge

    def merge(series, control, field)
      if field == "uuid"
        merge_uuids(series, control)
      elsif field == "name"
        merge_names(series, control)
      else
        raise ArgumentError.new "invalid merge argument: #{field.inspect}"
      end
    end

    def merge_uuids(series, control)
      new_series = { "" => {} }
      series.each do |uuid, name_series|
        name_series.each do |name, subseries|
          if skip_name?(control, name)
            subseries
          else
            new_series[""][name] ||= {}
            subseries.each do |ts,val|
              # add 1 usec until collision is past
              while new_series[""][name].has_key? ts
                ts = ts + 1
              end
              new_series[""][name][ts] = val
            end
          end
        end
      end
      return new_series, control
    end

    def merge_names(series, control)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] ||= { "" => {} }
        name_series.each do |name, subseries|
          if skip_name?(control, name)
            subseries
          else
            subseries.each do |ts,val|
              # add 1 usec until collision is past
              while new_series[uuid][""].has_key? ts
                ts = ts + 1
              end
              new_series[uuid][""][ts] = val
            end
          end
        end
      end
      return new_series, control
    end
  end
end
