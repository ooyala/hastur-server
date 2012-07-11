require "hastur-server/aggregation/base"

module Hastur
  module Aggregation
    class UnsupportedCompoundTypeError < StandardError ; end
    extend self
    @functions.merge! "compound" => :compound, "compound_list" => :compound_list

    FIELDS = {
      "linux.proc.stat" => %w[
         user nice system idle iowait irq softirq steal guest guest_nice
      ].freeze,
      "linux.proc.net.dev" => %w[
        rx_bytes rx_packets rx_errs rx_drop rx_fifo rx_frame rx_compressed rx_multicast
        tx_bytes tx_packets tx_errs tx_drop tx_fifo tx_frame tx_compressed tx_multicast
      ].freeze,
      # see: linux/Documentation/iostats.txt
      "linux.proc.diskstats" => %w[
        reads_completed reads_merged sectors_read read_milliseconds
        writes_completed writes_merged sectors_write write_milliseconds
        io_in_progress io_milliseconds io_milliseconds_weighted
      ].freeze
    }.freeze

    #
    # Extract keys from a plain hash stored in a compound value.
    #
    # @param [Hash] series
    # @return [Hash] series
    # @example
    #   /api/name/linux.proc.uptime?fun=compound(uptime,idle)
    #   /api/name/linux.proc.stat?fun=compound(processes,procs_running,procs_blocked)
    #
    def compound(series, *keys)
      keys.each do |key|
        map_expand series, key do |ts,val,name|
          sname = [name,key].join('.')
          { sname => { ts => val[key] } }
        end
      end
    end

    #
    # Expaned a compound value's key => Array into many names in the return set.
    #
    # @param [Hash] series
    # @return [Hash] series
    # @example
    #   /api/name/linux.proc.stat?fun=compound_list(cpu)
    #   /api/name/linux.proc.stat?fun=compound_list(cpu0,cpu1)
    #
    def compound_list(series, *keys)
      keys.each do |key|
        map_expand series, key do |ts,val,name|
          out = {}
          unless FIELDS.has_key? name
            raise UnsupportedCompoundTypeError.new "#{name} is not a supported compound type"
          end

          FIELDS[name].each_with_index do |sub,idx|
            sname = [name,key,sub].join('.')
            out[sname] ||= {}
            out[sname][ts] = val[key][idx]
          end
          out
        end
      end
    end

    #
    # @param [Hash] series
    # @param [Symbol,Numeric] seed how to seed the difference
    # @yield [Numeric, Numeric, ...] call the block with the current value, the state
    #        value, and any extra arguments passed into map_over.
    # @return [Hash] series
    #
    def map_expand(series, key)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, series|
          series.each do |ts,val|
            new_series[uuid].merge! yield ts, val, name
          end
        end
      end
      new_series
    end
  end
end
