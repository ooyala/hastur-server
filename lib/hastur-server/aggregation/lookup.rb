require "hastur-server/time_util"
require "hastur-server/aggregation/base"
require "hastur-server/cassandra/schema"
require "hastur-server/cassandra/derive"

module Hastur
  module Aggregation
    include Hastur::TimeUtil
    extend self
    attr_reader :cass_client
    attr_writer :start_ts, :end_ts
    @functions.merge! "cname" => :cname, "fqdn" => :fqdn, "hostname" => :hostname
    @cass_client = @start_ts = @end_ts = nil

    def cass_client=(client)
      @cass_client = client
    end

    def start_ts
      @start_ts || end_ts - USEC_TWO_DAYS
    end

    def end_ts
      @end_ts || usec_epoch
    end

    def hostname(series)
      fqdn(cname(series))
    end

    def cname(series)
      names = Hastur::Cassandra.network_names_for_uuids(cass_client, series.keys, start_ts, end_ts)
      series.keys.each do |uuid|
        if names[uuid][:cnames] and names[uuid][:cnames].any?
          series[names[uuid][:cnames][0]] = series.delete uuid
        end
      end
      series
    end

    def fqdn(series)
      names = Hastur::Cassandra.network_names_for_uuids(cass_client, series.keys, start_ts, end_ts)
      series.keys.each do |uuid|
        if names[uuid][:fqdn]
          series[names[uuid][:fqdn]] = series.delete uuid
        end
      end
      series
    end
  end
end
