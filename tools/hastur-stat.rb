#!/usr/bin/env ruby
#
# Prints something like this using data from the REST server.
# Still isn't working exactly as expected. Either the system stats aren't being
# reported as regularly as they're supposed to, or this script isn't exactly
# right yet. Also could be that data count=1 isn't always giving the right row.
# It's late, I'm going to bed now. (2012-05-26 1:52am)
#
#        hostname:    eth0_total     eth0_recv     eth0_send     iops_read     iops_write      1min  5min 15min  eth0_ts iops_ts
#    hastur-core1:         91581         31330         60250             0              1      0.31  0.45  0.53      170    170
#    hastur-core2:        120393         38713         81679             0              1      0.54  0.53  0.56      220    170
#    hastur-core2:        123932         39409         84523             0              1      0.49  0.52  0.56      210     10
#    hastur-core3:        330473         67129        263344             0              1       0.6  0.48  0.51       10     10
#    hastur-core1:        242937         63091        179846             0              1      0.37   0.5  0.56       10     10
#        hostname:    eth0_total     eth0_recv     eth0_send     iops_read     iops_write      1min  5min 15min  eth0_ts iops_ts
#    hastur-core2:        171690         50343        121347             0              1      0.28  0.51  0.56       50    220
#    hastur-core2:        130853         41562         89291             0              1      0.51  0.52  0.56      220    210
#    hastur-core2:        222706         65407        157298             0              1      0.32  0.51  0.56       40     10
#    hastur-core1:        132832         42680         90151             0              1      0.97  0.58  0.57      170    170
#    hastur-core2:        128756         42460         86296             0              1      0.57  0.54  0.56      210    210
#

require "httparty"

class Hstat
  include HTTParty
  format :json
  headers 'Accept-Encoding' => 'gzip,deflate'
  attr_reader :stats

  def initialize(uuids)
    @uuids = uuids
    @mutex = Mutex.new
    @threads = []
    @stats = {}
  end

  def run
    @running = true
    @uuids.each do |uuid, hostname|
      %w[stat net.dev loadavg diskstats].each do |name|
        @threads << Thread.new do
          begin
            loop do
              req = HTTParty.get("http://hastur.ooyala.com/api/node/#{uuid}/data/compound/linux.proc.#{name}?count=1&ago=five_minutes")
              data = MultiJson.load(req.body)
              @mutex.synchronize do
                @stats[uuid] ||= {}
                @stats[uuid][name] = data
              end
              sleep 2
            end
          rescue Exception => e
            STDERR.puts "exception: #{e}"
          end
        end
        sleep 0.2
      end
    end
  end

  def uuids(&block)
    @mutex.synchronize do
      @stats.each do |uuid,stats|
        block.call uuid, stats
      end
    end
  end

end

uuids = {
  "0eb214f0-8a95-11e1-aff2-1231391b2c02" => "hastur-core1",
  "079c8b32-8a95-11e1-a1b9-123138124754" => "hastur-core2",
  "8d2abad6-8a71-11e1-a950-12313d1e3623" => "hastur-core3",
}

HOSTNAME_PAD = 16
STAT_KEYS = %w[ eth0_send eth0_recv iops_read iops_write eth0_ts iops_ts ]

hstat = Hstat.new uuids
hstat.run

def mkhash(uuids, keys)
  shash = {}
  uuids.each do |uuid,|
    shash[uuid] = {}
    keys.each { |name| shash[uuid][name.to_sym] = 0 }
  end
  shash
end

def print_header
  printf "% #{HOSTNAME_PAD}s: % 13s % 13s % 13s  %12s   %12s     %5s %5s %5s  %06s %06s\n",
    *%w[ hostname eth0_total eth0_recv eth0_send iops_read iops_write 1min 5min 15min eth0_ts iops_ts ]
end

def print_stats(name, d, last)
  eth0_s = d[:eth0_elapsed] / 1_000_000
  iops_s = d[:iops_elapsed] / 1_000_000

  return last if eth0_s == last[0] or iops_s == last[1]

  printf "% #{HOSTNAME_PAD}s: % 13s % 13s % 13s  %12s   %12s     %5s %5s %5s   % 6d % 6d\n",
    name,
    (d[:eth0_recv] + d[:eth0_send]) / eth0_s,
    d[:eth0_recv] / eth0_s,  d[:eth0_send] / eth0_s,
    d[:iops_read] / iops_s,  d[:iops_write] / iops_s,
    d[:load_1min], d[:load_5min], d[:load_15min],
    eth0_s, iops_s

  [eth0_s, iops_s]
end

count = mkhash(uuids, [:eth0, :iops])
current = mkhash(uuids, STAT_KEYS)
diffs = mkhash(uuids, STAT_KEYS)
last_header = Time.now - 31
last_print = mkhash(uuids, [])

loop do
  sleep 2

  previous = current
  current = mkhash(uuids, STAT_KEYS)

  hstat.uuids do |uuid,stats|
    if stats["net.dev"]
      tskey = stats["net.dev"]["data"]["compound"]["linux.proc.net.dev"].keys.first
      eth0  = stats["net.dev"]["data"]["compound"]["linux.proc.net.dev"][tskey]["eth0"]

      current[uuid][:eth0_ts] = tskey.to_i
      current[uuid][:eth0_recv] = eth0[0]
      current[uuid][:eth0_send] = eth0[8]

      if current[uuid][:eth0_ts] > previous[uuid][:eth0_ts]
        count[uuid][:eth0]         = count[uuid][:eth0] + 1
        diffs[uuid][:eth0_ts]      = current[uuid][:eth0_ts]
        diffs[uuid][:eth0_elapsed] = current[uuid][:eth0_ts]   - previous[uuid][:eth0_ts]
        diffs[uuid][:eth0_recv]    = current[uuid][:eth0_recv] - previous[uuid][:eth0_recv]
        diffs[uuid][:eth0_send]    = current[uuid][:eth0_send] - previous[uuid][:eth0_send]
      end
    end

    if stats["loadavg"]
      lavg = stats["loadavg"]["data"]["compound"]["linux.proc.loadavg"].values.first
      diffs[uuid][:load_1min] = lavg[0]
      diffs[uuid][:load_5min] = lavg[1]
      diffs[uuid][:load_15min] = lavg[2]
    end

    if stats["diskstats"]
      tskey = stats["diskstats"]["data"]["compound"]["linux.proc.diskstats"].keys.first
      disks = stats["diskstats"]["data"]["compound"]["linux.proc.diskstats"][tskey]

      disks.each do |device,values|
        next unless device =~ /\A(?:sd|vd|xvd|hd)[a-z]\Z/
        current[uuid][:iops_read]  = current[uuid][:iops_read]  + disks[device][0]
        current[uuid][:iops_write] = current[uuid][:iops_write] + disks[device][7]
      end

      current[uuid][:iops_ts] = tskey.to_i

      if current[uuid][:iops_ts] > previous[uuid][:iops_ts]
        count[uuid][:iops]         = count[uuid][:iops] + 1
        diffs[uuid][:iops_ts]      = current[uuid][:iops_ts]
        diffs[uuid][:iops_elapsed] = current[uuid][:iops_ts]    - previous[uuid][:iops_ts]
        diffs[uuid][:iops_read]    = current[uuid][:iops_read]  - previous[uuid][:iops_read]
        diffs[uuid][:iops_write]   = current[uuid][:iops_write] - previous[uuid][:iops_write]
      end
    end
  end

  if Time.now - last_header > 30
    print_header
    last_header = Time.now
  end

  uuids.each do |uuid,hostname|
    next unless count[uuid][:iops] > 2 and count[uuid][:eth0] > 2
    last_print[uuid] = print_stats hostname, diffs[uuid], last_print[uuid]
  end
end

