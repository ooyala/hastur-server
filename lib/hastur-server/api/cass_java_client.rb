require "hastur-server/api/jruby-astyanax"

module Hastur
  module API
    # This uses JRuby-Astyanax to implement something that looks like the Cassandra gem client
    class CassandraJavaClient
      def initialize(uris)
        @ast_client = ::Astyanax::Client.new uris, 9160
        @keyspace = @ast_client.connect("hastur", :discovery => false)
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
      #   :ttl
      #   :consistency
      def insert(cf, row_key, cols, options = {})
        ast_cf = cf_for_name(cf)
        batch = @insert_batch || @java_keyspace.prepare_mutation_batch
        row = batch.with_row(ast_cf, row_key.to_java.bytes)

        cols.each do |name, val|
          unless val.kind_of?(String)
            raise "Value must be a string by the time it gets to Astyanax"
          end
          row.java_send(:putColumn, [java.lang.Object, Java::byte[], java.lang.Integer], name.to_java.bytes, val.to_java.bytes, options[:ttl])
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
