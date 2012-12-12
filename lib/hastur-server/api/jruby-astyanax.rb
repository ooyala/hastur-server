#
# A JRuby wrapper around Netflix's Astyanax Cassandra client
#
# A (hopefully) good example of JRuby-Java integration

require "java"

java_import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
java_import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl
java_import com.netflix.astyanax.AstyanaxContext
java_import com.netflix.astyanax.AuthenticationCredentials
java_import com.netflix.astyanax.retry.ExponentialBackoff
java_import com.netflix.astyanax.thrift.ThriftFamilyFactory
java_import com.netflix.astyanax.connectionpool.NodeDiscoveryType
java_import com.netflix.astyanax.serializers.StringSerializer
java_import com.netflix.astyanax.serializers.BytesArraySerializer
java_import com.netflix.astyanax.serializers.DateSerializer
java_import com.netflix.astyanax.serializers.CompositeSerializer

module Astyanax
  # TODO(ev): Move these constants/enums to a separate class
  # Astyanax::ConsistencyLevel::
  # CL_ONE, CL_QUORUM, CL_ALL, CL_ANY, CL_EACH_QUORUM, CL_LOCAL_QUORUM, CL_TWO, CL_THREE
  include_package "com.netflix.astyanax.model"

  class Client
    attr_accessor :seeds
    attr_accessor :port
    attr_accessor :username
    attr_accessor :password
    attr_accessor :cluster_name
    attr_reader :context

    # @param [List[String]] The list of seed Cassandra nodes.  The other nodes can be discovered
    #                       automatically.
    def initialize(seeds, port=9160, cluster_name="aCluster", username=nil, password=nil, threads=16)
      @executor_service = java.util.concurrent.Executors.newFixedThreadPool(threads)
      @seeds = seeds
      @port = port
      @username = username
      @password = password
      @cluster_name = cluster_name
    end

    # Connect to a keyspace, using default quorum read/write consistency and exponential backoff.
    # TODO(ev): Support more options in the future
    #
    # @option options Fixnum :initial_retry_delay_ms Retry delay in ms, default is 250
    # @option options Fixnum :max_retries Maximum number of retries, default is 5
    # @option options Fixnum :connect_timeout_ms Connection timeout threshold in ms, default is 2000
    # @option options Boolean :discovery Whether to do auto-node-discovery, default is true
    #
    def connect(keyspace, options={})
      initial_retry_delay_ms = options[:initial_retry_delay_ms] || 250
      max_retries = options[:max_retries] || 5
      connect_timeout_ms = options[:connect_timeout_ms] || 2000
      connection_config = ConnectionPoolConfigurationImpl.new(@cluster_name).
        setPort(@port.to_i).
        setSeeds(@seeds.join(",")).
        setMaxConnsPerHost(100).
        setMaxConns(1000).
        setConnectTimeout(connect_timeout_ms).
        setSocketTimeout(connect_timeout_ms).
        setMaxTimeoutWhenExhausted(connect_timeout_ms * max_retries)

      if @username
        connection_config.setAuthenticationCredentials(Credentials.new(@username, @password))
      end

      discovery = options[:discovery] ? NodeDiscoveryType::RING_DESCRIBE : NodeDiscoveryType::NONE

      @context = AstyanaxContext::Builder.new.
        forCluster(@cluster_name).
        forKeyspace(keyspace).
        withAstyanaxConfiguration(AstyanaxConfigurationImpl.new.
          setAsyncExecutor(@executor_service).
          setDefaultReadConsistencyLevel(Astyanax::ConsistencyLevel::CL_QUORUM).
          setRetryPolicy(ExponentialBackoff.new(initial_retry_delay_ms, max_retries)).
          setDiscoveryType(NodeDiscoveryType::RING_DESCRIBE)).
        withConnectionPoolConfiguration(connection_config).
        buildKeyspace(ThriftFamilyFactory.getInstance())

      @context.start()
      @keyspace = @context.getEntity()
      Keyspace.new(@keyspace)
    end

    # Shuts down the last context.  If you use connect to connect with different keyspaces, then you
    # need to individually invoke the context's shutdown method.
    def shutdown
      @context.shutdown()
    end
  end

  # A wrapper class for the Astyanax Keyspace class, offering easier to use get*() primitives
  class Keyspace
    DEFAULT_OPTIONS = {:rowkey_type => :bytes, :column_type => :bytes, :count => 1000}

    def initialize(astyanax_keyspace)
      @keyspace = astyanax_keyspace
    end

    def java_keyspace
      @keyspace
    end

    # A simple get row function that wraps a whole bunch of Astyanax functionality
    #
    # @param [String|ColumnFamily] columnfamily to read from, either a string or a ColumnFamily instance
    # @param [?] row key, default type is String but should match :rowkey_type
    # @param [Hash] options
    #                 :count        - max # of columns to retrieve, defaults to 1000
    #                 :start
    #                 :finish
    #                 :reversed    [Boolean] true to reverse column order
    #                 :consistency [Astyanax::ConsistencyLevel]
    #
    # @returns [Hash] A hash of column names to values
    #
    def get(column_family, rowkey, options)
      query, range = get_query_and_range(column_family, options)
      result = query.getKey(rowkey).withColumnRange(range.build()).execute().getResult()
      Hash[result.map { |c| [ String.from_java_bytes(c.name), String.from_java_bytes(c.byte_array_value) ] }]
    end

    # Reads from multiple keys at once
    #
    # @param [String|ColumnFamily] columnfamily to read from, either a string or a ColumnFamily instance
    # @param [List[?]] row keys, default type is String but should match :rowkey_type
    # @param [Hash] options  -- see options for get
    #
    # @returns [Hash] A hash of row keys to hashes of column names to values
    #
    def multiget(column_family, rowkeys, options)
      query, range = get_query_and_range(column_family, options)
      query = query.get_key_slice(rowkeys.to_java).with_column_range(range.build)

      result = query.execute.result
      result.map do |row|
        { String.from_java_bytes(row.key) => Hash[row.columns.map { |c| [ String.from_java_bytes(c.name), String.from_java_bytes(c.byte_array_value) ] }] }
      end.inject({}, &:merge)
    end

    # Reads from multiple keys at once, but doesn't make a Ruby-friendly final output structure.
    # This is very fast but can be awkward to use in Ruby.
    #
    # @param [String|ColumnFamily] columnfamily to read from, either a string or a ColumnFamily instance
    # @param [List[?]] row keys, default type is String but should match :rowkey_type
    # @param [Hash] options  -- see options for get
    #
    # @returns [Rows] An Astyanax Rows<byte[],byte[]> object
    #
    def raw_multiget(column_family, rowkeys, options)
      query, range = get_query_and_range(column_family, options)
      query = query.get_key_slice(rowkeys.to_java).with_column_range(range.build)

      query.execute.result
    end

    # Reads from multiple keys at once
    #
    # @param [String|ColumnFamily] columnfamily to read from, either a string or a ColumnFamily instance
    # @param [List[?]] row keys, default type is String but should match :rowkey_type
    # @param [Hash] options  -- see options for get
    #
    # @returns [Hash(key => ColumnList)]
    def multi_count_columns(column_family, rowkeys, options)
      # TODO: test me!
      query, range = get_query_and_range(column_family, options)
      Hash[ query.get_key_slice(*rowkeys).with_column_range(range.build()).getCount().execute().getResult().map do |row|
        [row.key, row.columns]
      end ]
    end

    private

    def get_query_and_range(column_family, options)
      merged_opts = DEFAULT_OPTIONS.merge(options)

      if not column_family.is_a? Astyanax::ColumnFamily
        column_family = Astyanax.get_column_family(column_family)
      end

      query = @keyspace.prepareQuery(column_family)
      if merged_opts[:consistency]
        query.setConsistencyLevel(merged_opts[:consistency])
      end

      range = com.netflix.astyanax.util.RangeBuilder.new
      range.setLimit(merged_opts[:count])
      range.setStart(merged_opts[:start].to_java_bytes) if merged_opts[:start]
      range.setEnd(merged_opts[:finish].to_java_bytes) if merged_opts[:finish]
      range.setReversed(true) if merged_opts[:reversed]

      [query, range]
    end
  end

  # Create the appropriate ColumnFamily instance.
  # One reason to do this is to reuse a ColumnFamily in multiple get() calls, instead of specifying a
  # string column_family and :rowkey_type or :column_type every time.
  #
  # Supported types:
  #   :text      - java.lang.String
  #   :bytes     - byte[]
  #   :date      - java.util.Date
  #   :composite
  def self.get_column_family(name)
    ColumnFamily.newColumnFamily(name, self.get_serializer(:bytes), self.get_serializer(:bytes))
  end

  # Parses a ColumnList containing composite columns.
  # @param [ColumnList] column_list the ColumnList as returned by Keyspace.get(...)
  # @param [List[Symbol]] column_types the types for each composite column component
  # @return [List[{:columns => [], :value => Object}]]
  def self.parse_composite_column_list(column_list, column_types)
    serializers = column_types.map { |col_type| self.get_serializer(col_type) }
    value_serializer = self.get_serializer(:bytes)
    column_list.map do |column|
      composite = column.getName
      col_components = (0..composite.size-1).map { |i| composite.get(i, serializers[i]) }
      {:columns => col_components, :value => column.getValue(value_serializer) }
    end
  end

  private

  def self.get_serializer(type)
    case type
    when :text
      StringSerializer.get()
    when :bytes
      BytesArraySerializer.get()
    when :date
      DateSerializer.get()
    when :composite
      CompositeSerializer.get()
    else
      raise "Unsupported type #{type}"
    end
  end

  class Credentials
    # This is how you implement a Java interface in JRuby
    include com.netflix.astyanax.AuthenticationCredentials

    def initialize(username, password)
      @username = username
      @password = password
    end

    def getUsername
      @username
    end

    def getPassword
      @password
    end

    def getAttributeNames() nil; end
    def getAttribute(name) nil; end
  end
end
