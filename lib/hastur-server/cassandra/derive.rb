require "cassandra/1.0"
require "hastur/api"
require "hastur-server/cassandra/schema"
require "hastur-server/time_util"
require "multi_json"

module Hastur
  module Cassandra
    class NoDataAvailableError < StandardError ; end
    include Hastur::TimeUtil
    extend self

    #
    # Assemble a hash of network names / hostnames for the given uuid(s).
    #
    # @param uuids UUID(s) to query for
    # @param start_ts
    # @param end_ts
    #
    def network_names_for_uuids(cass_client, uuids, start_ts, end_ts)
      tmp_start_ts = start_ts - USEC_ONE_DAY * 15
      cnames = Hastur::Cassandra.lookup_by_key cass_client, :cnames, start_ts, end_ts, :count => 1_000_000
      ohais  = Hastur::Cassandra.get cass_client, uuids, "info_ohai", tmp_start_ts, end_ts, :count => 1
      regs   = Hastur::Cassandra.get cass_client, uuids, "reg_agent", tmp_start_ts, end_ts, :count => 1

      unless ohais.keys.any? or regs.keys.any?
        raise NoDataAvailableError.new "None of #{uuids} have registered recently. Try restarting the agent(s)."
      end

      out = {}
      uuids.each do |uuid|
        sys = { :hostname => nil, :fqdn => nil, :nodename => nil, :cnames => [] }

        # first, try the registration information
        if regs[uuid] and regs[uuid]["reg_agent"]
          reg_ts, reg_json = regs[uuid]["reg_agent"][""].shift
          reg = MultiJson.load reg_json rescue {}

          # we only send the fqdn as hostname right now, need to add uname(2) fields
          # agent currently sends :hostname => Socket.gethostname
          sys[:hostname] = reg["hostname"]
          sys[:nodename] = reg["nodename"]

          # /etc/cnames is an Ooyala standard for setting the system's human-facing name
          if reg["etc_cnames"]
            sys[:cnames] = reg["etc_cnames"]
          end
        end

        # use ohai to fill in additional info, including EC2 info
        if ohais[uuid] and ohais[uuid]["info_ohai"]
          ohai_ts, ohai_json = ohais[uuid]["info_ohai"][""].shift
          ohai = MultiJson.load ohai_json rescue {}

          # ohai's 'hostname' is useless, it uses hostname -s to get it
          sys[:hostname] ||= ohai["fqdn"]
          sys[:fqdn]     ||= ohai["fqdn"]

          if ohai["ec2"]
            # use the EC2 info regardless of what the OS says
            sys[:hostname] = ohai["ec2"]["local_hostname"]
            sys[:fqdn]     = ohai["ec2"]["public_hostname"]
          end
        end

        # hosts can have any number of cnames
        sys.values.each do |name|
          if cnames.has_key? name
            sys[:cnames] << cnames[name]
          end
        end
        # don't sort! etc_cnames values should always come first, alphabetical is useless
        sys[:cnames] = sys[:cnames].uniq

        # provide a simple array of all known network names
        # reverse the flattened list so the cnames come first
        sys[:all] = sys.values.flatten.compact.reverse.uniq

        out[uuid] = sys
      end

      out
    end

    #
    # Get the UUID for given hostnames. This relies on a quite a number of datapoints coming
    # together and is strictly best-effort. At a minimum the node will have to have registered
    # and ideally have a sane hostname to start with. The lookup table must also be up-to-date
    # and is managed by an external scheduler.
    #
    # @param [Array<String>] hostnames to translate
    # @param [Fixnum] start_ts
    # @param [Fixnum] end_ts
    #
    def uuids_for_hostnames(cass_client, hostnames, start_ts, end_ts)
      lookup = Hastur::Cassandra.lookup_by_key(cass_client, "host-uuid", start_ts, end_ts)

      # just rely on the lookup table and sink most of the logic there in a scheduled job
      out = {}
      hostnames.each do |host|
        out[host] = lookup[host]
      end

      out
    end
  end
end
