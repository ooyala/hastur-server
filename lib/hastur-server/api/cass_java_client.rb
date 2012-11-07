require "jruby-astyanax"

module Hastur
  module API
    # This uses JRuby-Astyanax to implement something that looks like the Cassandra gem client
    class CassandraJavaClient
      def initialize(uris)
        @ast_client = ::Astyanax::Client.new uris, 9160, :discovery => false
        @keyspace = @ast_client.connect("hastur")
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
        row = batch.with_row(ast_cf, row_key)

        cols.each { |name, val| row.put_column(name, val, options[:ttl]) }

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

        ast_options = options.dup
        ast_options[:consistency] = consistency_for(options[:consistency])
        @keyspace.get(cf, row_key, ast_options)
      end

      # Options:
      #   :count
      #   :consistency
      #   :start
      #   :finish
      #   :reversed
      def multi_get(cf, rows, options = {})
        ast_cf = cf_for_name(cf)

        ast_options = options.dup
        ast_options[:consistency] = consistency_for(options[:consistency])
        @keyspace.multiget(cf, row_key, ast_options)
      end

      # Options:
      #   :count
      #   :consistency
      #   :start
      #   :finish
      #   :reversed
      def multi_count_columns(cf, rows, options = {})
        ast_cf = cf_for_name(cf)

        ast_options = options.dup
        ast_options[:consistency] = consistency_for(options[:consistency])
        @keyspace.multi_count_columns(cf, row_key, ast_options)
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
        @cfs[name] = ::Astyanax.get_column_family(name, :bytes, :bytes)
      end

      def consistency_for(ruby_consistency)
        case ruby_consistency
        when ::Hastur::Cassandra::CONSISTENCY_ONE
          ::Astyanax::ConsistencyLevel::CL_ONE
        when ::Hastur::Cassandra::CONSISTENCY_TWO
          raise "No such Astyanax constant for Cass gem consistency level!"
        when ::Hastur::Cassandra::CONSISTENCY_ZERO
          raise "No such Astyanax constant for Cass gem consistency level!"
        when ::Hastur::Cassandra::CONSISTENCY_QUORUM
          ::Astyanax::ConsistencyLevel::CL_QUORUM
        when ::Hastur::Cassandra::CONSISTENCY_ALL
          ::Astyanax::ConsistencyLevel::CL_ALL
        else
          raise "No such Astyanax constant for Cass gem consistency level!"
        end
      end
    end
  end
end
