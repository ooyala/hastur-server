require "hastur-server/aggregation/base"

# Hastur agent sends a bunch of data from /proc and /sys as compound datastructures every 10 seconds.
# Sending /proc/stat as individual stats would result in almost 100 stats every 10s on an 8-core machine
# rather than 1. While this format is much harder to work with, it's /considerably/ more efficient at every
# step until it's time to retrieve. These functions have domain-specific knowledge required to make compound
# values useful while retaining their compactness until the last moment.
#
# Currently only agents on Linux send compound stats.

module Hastur
  module Aggregation
    class UnsupportedCompoundTypeError < StandardError ; end
    extend self
    @functions.merge! "compound" => :compound, "compound_list" => :compound_list

    FIELDS = {
      # /proc/stat CPU rows are stored as arrays
      "linux.proc.stat" => %w[
         user nice system idle iowait irq softirq steal guest guest_nice
      ].freeze,
      # /proc/net/dev is stored as a hash of arrays, :iface => [ values ]
      "linux.proc.net.dev" => %w[
        rx_bytes rx_packets rx_errs rx_drop rx_fifo rx_frame rx_compressed rx_multicast
        tx_bytes tx_packets tx_errs tx_drop tx_fifo tx_frame tx_compressed tx_multicast
      ].freeze,
      # similarly with diskstats, but :device => [ values ]
      # see: linux/Documentation/iostats.txt
      "linux.proc.diskstats" => %w[
        reads_completed reads_merged sectors_read read_milliseconds
        writes_completed writes_merged sectors_write write_milliseconds
        io_in_progress io_milliseconds io_milliseconds_weighted
      ].freeze,
      # /proc/loadavg a simple array from splitting on whitespace
      # the translation below splits lwp, the agent sends it as an unmodified string :(
      "linux.proc.loadavg" => %w[ 1min 5min 15min lwp_running lwp_total npid ].freeze
    }.freeze

    # automatically normalize troublesome structures
    TRANSLATE = {
      # remap load average from an array to a hash and split the lwp string
      "linux.proc.loadavg" => proc { |a|
        out = {}
        %w[1min 5min 15min lwp npid].each_with_index do |name,idx|
          out[name] = a[idx]
        end
        out["lwp_running"], out["lwp_total"] = out.delete("lwp").split('/').map(&:to_i)
        out
      },

      # let compound() auto-explode /proc/stat which is where we get CPU usage
      "linux.proc.stat" => proc { |sample|
        sample.keys.each do |key|
          if key.start_with? "cpu"
            row = sample.delete(key)
            FIELDS["linux.proc.stat"].each_with_index do |field, idx|
              sample["#{key}.#{field}"] = row[idx] if row[idx]
            end
          end
        end
        sample
      }
    }

    #
    # Extract keys from a plain hash stored in a compound value. This is only really valid
    # as the inner-most function, as this is how compounds are translated to work well with
    # all the other functions.
    #
    # @param [Hash] series
    # @return [Hash] series
    # @example
    #   /api/name/linux.proc.uptime?fun=compound(uptime,idle)
    #   /api/name/linux.proc.stat?fun=compound(processes,procs_running,procs_blocked)
    #
    def compound(series, control, *keys)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, subseries|
          # default to all subkeys of the compound value if no keys were specified by the user
          if keys.none? and FIELDS[name]
            nkeys = FIELDS[name]
          else
            nkeys = keys
          end

          # procs are defined above for exploding / translating compact lists into key/value pairs
          if TRANSLATE[name]
            subseries.each do |ts,val|
              subseries[ts] = TRANSLATE[name].call val
            end

            # the list of keys is variable per-system, unless given a list of keys,
            # set the list of keys to match what was seen on the system
            nkeys = subseries.first.last.keys

            # but if the user has said which key they want, try to return just what they requested
            # only allow matching on the first field though, not the expanded fields
            # e.g. compound(cpu) is ok but compound(cpu.user) isn't
            if keys.any?
              nkeys = nkeys.select { |nk| keys.include?(nk.split('.')[-2]) or keys.include?(nk) }
            end
          end

          # now flatten the key/values into many series for output
          nkeys.each do |key|
            new_name = "#{name}.#{key}"
            new_series[uuid][new_name] = {}
            subseries.each do |ts,hash|
              # a bug in agent versions < 0.24 puts some fields in arrays that should be, fix on the fly
              # TODO(al) remove hack after 2012-10-01 or so
              new_series[uuid][new_name][ts] = hash[key].respond_to?(:pop) ? hash[key].pop : hash[key]
            end
          end
        end
      end
      return new_series, control
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
    def compound_list(series, control, *keys)
      new_series = {}
      series.each do |uuid, name_series|
        new_series[uuid] = {}
        name_series.each do |name, subseries|
          out = {}
          unless FIELDS.has_key? name
            raise UnsupportedCompoundTypeError.new "#{name} is not a supported compound type"
          end

          keys.each do |key|
            # initialize the new series
            FIELDS[name].each do |field|
              new_series[uuid]["#{name}.#{key}.#{field}"] ||= {}
            end
            subseries.each do |ts,hash|
              FIELDS[name].each_with_index do |field, idx|
                new_series[uuid]["#{name}.#{key}.#{field}"][ts] = hash[key][idx]
              end
            end
          end
        end
      end
      return new_series, control
    end
  end
end
