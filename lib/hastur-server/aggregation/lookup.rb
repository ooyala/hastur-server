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
    @functions.merge! "cname" => :cname
    @cass_client = @start_ts = @end_ts = nil

    def cass_client=(client)
      @cass_client = client
    end

    def start_ts
      @start_ts || end_ts - USEC_TWO_DAYS
    end

    def end_ts
      @end_ts || epoch_usec
    end

    def cname(series)
      names = network_names_for_uuids(series.keys, start_ts, end_ts)
      series.keys.each do |uuid|
        if names[uuid][:cnames] and names[uuid][:cnames].any?
          series[names[uuid][:cnames][0]] = series.delete uuid
        end
      end
    end

    def fqdn(series)
      names = network_names_for_uuids(series.keys, start_ts, end_ts)
      series.keys.each do |uuid|
        if names[uuid][:fqdn] and names[uuid][:fqdn].any?
          series[names[uuid][:fqdn][0]] = series.delete uuid
        end
      end
    end
  end
end
