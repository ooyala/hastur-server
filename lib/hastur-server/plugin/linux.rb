require "hastur/api"

module Hastur
  module Plugin
    module Linux
      extend self

      def run
        ts = Hastur.timestamp # use the same timestamp so they can be lined up easily
        Hastur.send :compound, "linux.proc.stat",      proc_stat,      ts
        Hastur.send :compound, "linux.proc.diskstats", proc_diskstats, ts
        Hastur.send :compound, "linux.proc.uptime",    proc_uptime,    ts
        Hastur.send :compound, "linux.proc.loadavg",   proc_loadavg,   ts
        Hastur.send :compound, "linux.proc.net.dev",   proc_net_dev,   ts
        Hastur.send :compound, "linux.proc.meminfo",   proc_meminfo,   ts
      end

      def proc_stat
        stats = Hash.new
        File.readlines('/proc/stat').each do |line|
          fields = line.split ' '
          key = fields.shift
          stats[key] = fields.map(&:to_i)

          # flatten interrupts to a single value, it's a big list and largely useless as-is
          # without a mapping, but the single value is useful for rate over time
          if key == "intr"
            stats[key] = stats[key].inject(&:+)
          end
        end
        stats
      end

      def proc_diskstats
        stats = Hash.new
        File.readlines('/proc/diskstats').each do |line|
          fields = line.split ' '
          key = fields.delete_at 2
          stats[key] = fields.map(&:to_i)
        end
        stats
      end

      def proc_uptime
        uptime, idle = File.readlines('/proc/uptime').first.chomp.split(' ').map(&:to_i)
        { "uptime" => uptime, "idle" => idle }
      end

      def proc_loadavg
        f = File.readlines('/proc/loadavg').first.chomp.split(' ')
        [ f[0].to_f, f[1].to_f, f[2].to_f, f[3], f[4].to_i ]
      end

      def proc_net_dev
        stats = Hash.new
        File.readlines('/proc/net/dev').map(&:chomp).map(&:strip).each do |line|
          values = line.split(/\s+/)
          if values[0].end_with? ':'
            values[0].chop!
            stats[values.shift] = values.map(&:to_i)
          end
        end
        stats
      end

      def proc_meminfo
        stats = Hash.new
        File.readlines('/proc/meminfo').map(&:chomp).each do |line|
          fields = line.split(/\s+/)
          fields[0].chop! # remove :
          # ignore size, it's always kB and probably always will be
          stats[fields[0]] = fields[1].to_i
        end
        stats
      end
    end
  end
end

