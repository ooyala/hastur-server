require "hastur-server/api/jruby-astyanax"

module Hastur
  module API
    # This uses JRuby-Astyanax to implement something that looks like the Cassandra gem client
    class CassandraJavaClient
      def initialize(uris)
        default_port = ENV["HASTUR_CASS_PORT"] || 9160
        @ast_client = ::Astyanax::Client.new uris, default_port.to_i,
          ENV["HASTUR_CASS_CLUSTER"] || "aCluster", ENV["HASTUR_CASS_USER"], ENV["HASTUR_CASS_PASSWD"]
        @keyspace = @ast_client.connect("hastur", :discovery => true, :connect_timeout_ms => 10_000)
        @java_keyspace = @keyspace.java_keyspace
        @cfs = {}
        @batch = nil
      end

      def batch(&block)
        raise "No nested batches!" if @batch

        begin
          @batch = true
          @insert_batch = @java_keyspace.prepare_mutation_batch
          yield(self)
          @insert_batch.execute
        ensure
          @insert_batch = nil
          @batch = nil
        end
      end

      # Options:
      #   :ttl_seconds
      #   :consistency
      def insert(cf, row_key, cols, options = {})
        ast_cf = cf_for_name(cf)
        batch = @insert_batch || @java_keyspace.prepare_mutation_batch
        row = batch.with_row(ast_cf, row_key.to_java_bytes)

        cols.each do |name, val|
          if val.kind_of?(String)
            # May be Java bytes already since they come from MessagePack
            val = val.to_java_bytes
          elsif val.kind_of?(Java::byte[])
            # Use unmodified
          elsif !val.kind_of?(String)
            raise "Value must be a string, not #{val.inspect}, by the time it gets to Astyanax"
          end
          row.java_send(:putColumn, [java.lang.Object, Java::byte[], java.lang.Integer], name.to_java_bytes, val, options[:ttl_seconds])
        end

        batch.execute unless @insert_batch
      end

      # Options:
      #   :count
      #   :consistency
      #   :start
      #   :finish
      #   :reversed
      def get(cf, row_key, options = {})
        ast_cf = cf_for_name(cf)

        @keyspace.get(cf, row_key.to_java_bytes, ast_options(options))
      end

      # Options:
      #   :count
      #   :consistency
      #   :start
      #   :finish
      #   :reversed
      def multi_get(cf, rows, options = {})
        ast_cf = cf_for_name(cf)

        @keyspace.multiget(cf, rows.map(&:to_java_bytes), ast_options(options))
      end

      # Options:
      #   :count
      #   :consistency
      #   :start
      #   :finish
      #   :reversed
      def raw_multi_get(cf, rows, options = {})
        ast_cf = cf_for_name(cf)

        @keyspace.raw_multiget(cf, rows.map(&:to_java_bytes), ast_options(options))
      end

      def raw_row_col_get(cf, row_hash, options = {})
        ast_cf = cf_for_name(cf)

        @keyspace.raw_row_col_get(ast_cf, row_hash, ast_options(options))
      end

      # Options:
      #   :count
      #   :consistency
      #   :start
      #   :finish
      #   :reversed
      def multi_count_columns(cf, rows, options = {})
        ast_cf = cf_for_name(cf)

        @keyspace.multi_count_columns(cf, rows.map(&:to_java_bytes), ast_options(options))
      end

      # Raise exception if can't connect
      def status_check
        # Fake get to check for exception
        # TODO(noah): test
        get "gauge_archive", " "
      end

      private

      def cf_for_name(name)
        raise "Column family cannot be empty!" if name.nil? || name.empty?
        name = name.to_s
        return @cfs[name] if @cfs[name]
        @cfs[name] = ::Astyanax.get_column_family(name)
      end

      def ast_options(options)
        merged_options = {
          :consistency => ::Hastur::Cassandra::CONSISTENCY_ONE,
          :count => 10_000,
        }.merge(options)
        merged_options[:consistency] = consistency_for(merged_options[:consistency])

        merged_options
      end

      HASTUR_TO_ASTYANAX_CONSISTENCY = {
        ::Hastur::Cassandra::CONSISTENCY_ONE => ::Astyanax::ConsistencyLevel::CL_ONE,
        ::Hastur::Cassandra::CONSISTENCY_TWO => ::Astyanax::ConsistencyLevel::CL_TWO,
        ::Hastur::Cassandra::CONSISTENCY_THREE => ::Astyanax::ConsistencyLevel::CL_THREE,
        ::Hastur::Cassandra::CONSISTENCY_QUORUM => ::Astyanax::ConsistencyLevel::CL_QUORUM,
        ::Hastur::Cassandra::CONSISTENCY_EACH_QUORUM => ::Astyanax::ConsistencyLevel::CL_EACH_QUORUM,
        ::Hastur::Cassandra::CONSISTENCY_LOCAL_QUORUM => ::Astyanax::ConsistencyLevel::CL_LOCAL_QUORUM,
        ::Hastur::Cassandra::CONSISTENCY_ALL => ::Astyanax::ConsistencyLevel::CL_ALL,
        ::Hastur::Cassandra::CONSISTENCY_ANY => ::Astyanax::ConsistencyLevel::CL_ANY,
      }
      def consistency_for(ruby_consistency)
        java_consistency = HASTUR_TO_ASTYANAX_CONSISTENCY[ruby_consistency]
        unless java_consistency
          raise "Unknown Astyanax constant for Cass gem consistency level: #{ruby_consistency.inspect}"
        end

        java_consistency
      end
    end
  end
end
