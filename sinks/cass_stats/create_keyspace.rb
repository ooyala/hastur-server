#!/usr/bin/env ruby

require "rubygems"
require "java"
require "trollop"

opts = Trollop::options do
  opt :host, :default => "127.0.0.1", :type => String
  opt :keyspace, :default => "Hastur", :type => String
end

# Require all the Hector jar files
Dir["hector-core-1.0-3/*.jar"].each do |f|
  require f
end

java_import me.prettyprint.hector.api

newKeyspace = HFactory.createKeyspaceDefinition("MyKeyspace",                 
                                                ThriftKsDef.DEF_STRATEGY_CLASS,  
                                                replicationFactor, 
                                                Arrays.asList(cfDef));
