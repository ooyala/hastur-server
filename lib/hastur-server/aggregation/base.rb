module Hastur
  module Aggregation
    extend self

    @functions ||= {}
    @functions.merge!({
      "include" => :incl,
      "exclude" => :excl,
      "all"     => :all,
      "delete"  => :delete,
    })

    #
    # Map over a series and yield the given proc for each entry, tracking
    # state using the tuple returned by the proc.
    #
    # @param [Hash] series
    # @param [Symbol,Numeric] seed how to seed the difference
    #   :first peek at the first value to initialize the state variable
    #   :shift pop the first entry in the series to init the state
    #   Numeric use the given number to init the state
    # @param [Array<Object>] *args additional arguments
    # @yield [Numeric, Numeric, ...] call the block with the current value, the state
    #        value, and any extra arguments passed into map_over.
    # @return [Hash] series
    #
    def map_over(series, control, seed, *args)
      each_subseries_in series, control do |name, subseries|
        case seed
          when :shift  ; state = subseries.delete(subseries.first.first)
          when :first  ; state = subseries[subseries.first.first]
          when Numeric ; state = seed
        end

        new_subseries = {}
        subseries.each do |ts,val|
          new_subseries[ts], state = yield val, state, *args
        end
        new_subseries
      end
    end

    #
    # Wrap up some boilerplate loops for rewriting one level shallower than map_over.
    #
    # @param [Hash] series
    # @yield name, subseries
    # @yieldreturn [Object] data to be placed under new_series[key][name]
    # will drop the name from the results entirely if false/nil
    #
    def each_subseries_in(series, control)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, subseries|
          if skip_name?(control, name)
            new_series[uuid][name] = subseries
          else
            new_series[uuid][name] = yield name, subseries
            unless new_series[uuid][name]
              new_series[uuid].delete name
            end
          end
        end
      end
      return new_series, control
    end

    #
    # Select particular series name(s) for the downstream functions to work on. Unselected
    # names will be ignored/unmodified but remain in the final output.
    #
    # @todo support wildcards
    # @example
    #   /api/name/linux.proc.stat/value?fun=derivative(incl(processes,compound(processes,procs_running,procs_blocked)))
    #
    def incl(series, control, *names)
      control[:include] ||= []
      control[:include].concat names
      return series, control
    end

    #
    # Select particular series name(s) for the downstream functions to ignore. Excluded
    # names will be ignored/unmodified but remain in the final output.
    #
    # @todo support wildcards
    # @example
    #   /api/name/linux.proc.stat/value?fun=derivative(excl(processes,compound(processes,procs_running,procs_blocked)))
    #
    def excl(series, control, *names)
      control[:exclude] ||= []
      control[:exclude].concat names
      return series, control
    end

    #
    # Clear any selections from include()/exclude() for downstream functions.
    #
    def all(series, control, ignore=nil)
      control.delete :include
      control.delete :exclude
      return series, control
    end

    #
    # Delete the given name/series from each uuid's set of series. It will be removed from the output entirely
    # and no further processing will apply.  Wildcards are not supported yet.
    # @todo support wildcards
    #
    def delete(series, control, *names)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.keys.each do |name|
          unless names.include?(name)
            new_series[uuid][name] = series[uuid][name]
          end
        end
      end
      return new_series, control
    end

    #
    # Test if a given name should be skipped according to include/exclude rules. Skipped subseries are
    # left unmodified.
    #
    def skip_name?(control, name)
      if control.has_key?(:exclude) and control[:exclude].include? name
        true
      elsif control.has_key?(:include) and not control[:include].include? name
        true
      end
    end
  end
end
