# Disable Rake-environment-task framework detection by uncommenting/setting to false
Warbler.framework_detection = false

# Warbler web application assembly configuration file
Warbler::Config.new do |config|
  # Features: additional options controlling how the jar is built.
  # Currently the following features are supported:
  # - gemjar: package the gem repository in a jar file in WEB-INF/lib
  # - executable: embed a web server and make the war executable
  # - compiled: compile .rb files to .class files
  config.features = %w(executable)

  # Application directories to be included in the webapp.
  config.dirs = %w(lib vendor tools)

  # Additional files/directories to include, above those in config.dirs
  # config.includes = FileList["db"]

  # Additional files/directories to exclude
  # config.excludes = FileList["lib/tasks/*"]

  # Additional Java .jar files to include.  Note that if .jar files are placed
  # in lib (and not otherwise excluded) then they need not be mentioned here.
  # JRuby and JRuby-Rack are pre-loaded in this list.  Be sure to include your
  # own versions if you directly set the value
  #config.java_libs += FileList["warbler_jars/*.jar"]
  # Stolen from .jbundler/classpath.rb - how will we do this in the long term?  NOT LIKE THIS.
  config.java_libs << '/Users/noah/.m2/repository/com/netflix/astyanax/astyanax/1.0.3/astyanax-1.0.3.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/cassandra/cassandra-all/1.0.8/cassandra-all-1.0.8.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/xerial/snappy/snappy-java/1.0.4.1/snappy-java-1.0.4.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/ning/compress-lzf/0.8.4/compress-lzf-0.8.4.jar'
  config.java_libs << '/Users/noah/.m2/repository/commons-cli/commons-cli/1.1/commons-cli-1.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/commons-codec/commons-codec/1.2/commons-codec-1.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/googlecode/concurrentlinkedhashmap/concurrentlinkedhashmap-lru/1.2/concurrentlinkedhashmap-lru-1.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/antlr/antlr/3.2/antlr-3.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/antlr/antlr-runtime/3.2/antlr-runtime-3.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/antlr/stringtemplate/3.2/stringtemplate-3.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/antlr/antlr/2.7.7/antlr-2.7.7.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/cassandra/deps/avro/1.4.0-cassandra-1/avro-1.4.0-cassandra-1.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/thoughtworks/paranamer/paranamer/2.2/paranamer-2.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/thoughtworks/paranamer/paranamer-ant/2.2/paranamer-ant-2.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/thoughtworks/paranamer/paranamer-generator/2.2/paranamer-generator-2.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/thoughtworks/qdox/qdox/1.10.1/qdox-1.10.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/asm/asm/3.2/asm-3.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/ant/ant/1.7.1/ant-1.7.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/ant/ant-launcher/1.7.1/ant-launcher-1.7.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/velocity/velocity/1.6.4/velocity-1.6.4.jar'
  config.java_libs << '/Users/noah/.m2/repository/commons-collections/commons-collections/3.2.1/commons-collections-3.2.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/jboss/netty/netty/3.2.1.Final/netty-3.2.1.Final.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/mortbay/jetty/jetty/6.1.22/jetty-6.1.22.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/mortbay/jetty/jetty-util/6.1.22/jetty-util-6.1.22.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/mortbay/jetty/servlet-api/2.5-20081211/servlet-api-2.5-20081211.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/codehaus/jackson/jackson-core-asl/1.4.0/jackson-core-asl-1.4.0.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/codehaus/jackson/jackson-mapper-asl/1.4.0/jackson-mapper-asl-1.4.0.jar'
  config.java_libs << '/Users/noah/.m2/repository/jline/jline/0.9.94/jline-0.9.94.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/yaml/snakeyaml/1.6/snakeyaml-1.6.jar'
  config.java_libs << '/Users/noah/.m2/repository/log4j/log4j/1.2.16/log4j-1.2.16.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/slf4j/slf4j-log4j12/1.6.1/slf4j-log4j12-1.6.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/thrift/libthrift/0.6.1/libthrift-0.6.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/junit/junit/4.4/junit-4.4.jar'
  config.java_libs << '/Users/noah/.m2/repository/javax/servlet/servlet-api/2.5/servlet-api-2.5.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/httpcomponents/httpclient/4.0.1/httpclient-4.0.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/httpcomponents/httpcore/4.0.1/httpcore-4.0.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/cassandra/cassandra-thrift/1.0.8/cassandra-thrift-1.0.8.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/github/stephenc/jamm/0.2.5/jamm-0.2.5.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/codehaus/jettison/jettison/1.3.1/jettison-1.3.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/stax/stax-api/1.0.1/stax-api-1.0.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/joda-time/joda-time/2.0/joda-time-2.0.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/github/stephenc/high-scale-lib/high-scale-lib/1.1.1/high-scale-lib-1.1.1.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/apache/servicemix/bundles/org.apache.servicemix.bundles.commons-csv/1.0-r706900_3/org.apache.servicemix.bundles.commons-csv-1.0-r706900_3.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/google/guava/guava/11.0.2/guava-11.0.2.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/google/code/findbugs/jsr305/1.3.9/jsr305-1.3.9.jar'
  config.java_libs << '/Users/noah/.m2/repository/com/github/stephenc/eaio-uuid/uuid/3.2.0/uuid-3.2.0.jar'
  config.java_libs << '/Users/noah/.m2/repository/org/slf4j/slf4j-api/1.6.4/slf4j-api-1.6.4.jar'

  # Loose Java classes and miscellaneous files to be included.
  # config.java_classes = FileList["target/classes/**.*"]

  # One or more pathmaps defining how the java classes should be copied into
  # the archive. The example pathmap below accompanies the java_classes
  # configuration above. See http://rake.rubyforge.org/classes/String.html#M000017
  # for details of how to specify a pathmap.
  # config.pathmaps.java_classes << "%{target/classes/,}p"

  # Bundler support is built-in. If Warbler finds a Gemfile in the
  # project directory, it will be used to collect the gems to bundle
  # in your application. If you wish to explicitly disable this
  # functionality, uncomment here.
  config.bundler = false

  # An array of Bundler groups to avoid including in the war file.
  # Defaults to ["development", "test"].
  # config.bundle_without = []

  # Other gems to be included. If you don't use Bundler or a gemspec
  # file, you need to tell Warbler which gems your application needs
  # so that they can be packaged in the archive.
  # For Rails applications, the Rails gems are included by default
  # unless the vendor/rails directory is present.
  # config.gems += ["activerecord-jdbcmysql-adapter", "jruby-openssl"]
  # config.gems << "tzinfo"

  config.gems += [
    "sinatra",
    "httparty",
    "yajl-ruby",
    "ffi-rzmq",
    "trollop",
    "uuid",
    "termite",
    "bluepill",
    "rainbow",
    "msgpack",
    "pony",
    "pry",
    "ohai",
    "sys-uname",
  ]

  config.gems["multi_json"] = "~>1.3.2"
  config.gems["hastur"] = "~>1.2.8"
  config.gems["hastur-rack"] = "~>0.0.10"
  config.gems["jruby-astyanax"] = "~>0.0.4"

  # Uncomment this if you don't want to package rails gem.
  config.gems -= ["rails"]

  # The most recent versions of gems are used.
  # You can specify versions of gems by using a hash assignment:
  # config.gems["rails"] = "2.3.10"

  # You can also use regexps or Gem::Dependency objects for flexibility or
  # finer-grained control.
  # config.gems << /^merb-/
  # config.gems << Gem::Dependency.new("merb-core", "= 0.9.3")

  # Include gem dependencies not mentioned specifically. Default is
  # true, uncomment to turn off.
  config.gem_dependencies = false

  # Array of regular expressions matching relative paths in gems to be
  # excluded from the war. Defaults to empty, but you can set it like
  # below, which excludes test files.
  # config.gem_excludes = [/^(test|spec)\//]

  # Pathmaps for controlling how application files are copied into the archive
  # config.pathmaps.application = ["WEB-INF/%p"]

  # Name of the archive (without the extension). Defaults to the basename
  # of the project directory.
  # config.jar_name = "mywar"

  # Name of the MANIFEST.MF template for the war file. Defaults to a simple
  # MANIFEST.MF that contains the version of Warbler used to create the war file.
  # config.manifest_file = "config/MANIFEST.MF"

  # When using the 'compiled' feature and specified, only these Ruby
  # files will be compiled. Default is to compile all \.rb files in
  # the application.
  # config.compiled_ruby_files = FileList['app/**/*.rb']

  # === War files only below here ===

  # Path to the pre-bundled gem directory inside the war file. Default
  # is 'WEB-INF/gems'. Specify path if gems are already bundled
  # before running Warbler. This also sets 'gem.path' inside web.xml.
  # config.gem_path = "WEB-INF/vendor/bundler_gems"

  # Files for WEB-INF directory (next to web.xml). This contains
  # web.xml by default. If there is an .erb-File it will be processed
  # with webxml-config. You may want to exclude this file via
  # config.excludes.
  # config.webinf_files += FileList["jboss-web.xml"]

  # Files to be included in the root of the webapp.  Note that files in public
  # will have the leading 'public/' part of the path stripped during staging.
  # config.public_html = FileList["public/**/*", "doc/**/*"]

  # Pathmaps for controlling how public HTML files are copied into the .war
  # config.pathmaps.public_html = ["%{public/,}p"]

  # Embedded webserver to use with the 'executable' feature. Currently supported
  # webservers are:
  # * <tt>winstone</tt> (default) - Winstone 0.9.10 from sourceforge
  # * <tt>jenkins-ci.winstone</tt> - Improved Winstone from Jenkins CI
  # * <tt>jetty</tt> - Embedded Jetty from Eclipse
  config.webserver = 'jetty'

  # Value of RAILS_ENV for the webapp -- default as shown below
  # config.webxml.rails.env = ENV['RAILS_ENV'] || 'production'

  # Application booter to use, one of :rack, :rails, or :merb (autodetected by default)
  # config.webxml.booter = :rails
  config.webxml.booter = :rack

  # Set JRuby to run in 1.9 mode.
  config.webxml.jruby.compat.version = "1.9"

  # When using the :rack booter, "Rackup" script to use.
  # - For 'rackup.path', the value points to the location of the rackup
  # script in the web archive file. You need to make sure this file
  # gets included in the war, possibly by adding it to config.includes
  # or config.webinf_files above.
  # - For 'rackup', the rackup script you provide as an inline string
  #   is simply embedded in web.xml.
  # The script is evaluated in a Rack::Builder to load the application.
  # Examples:
  # config.webxml.rackup.path = 'WEB-INF/hello.ru'
  config.webxml.rackup.path = 'WEB-INF/config_v2.ru'
  # config.webxml.rackup = %{require './lib/demo'; run Rack::Adapter::Camping.new(Demo)}
  # config.webxml.rackup = require 'cgi' && CGI::escapeHTML(File.read("config.ru"))

  # Control the pool of Rails runtimes. Leaving unspecified means
  # the pool will grow as needed to service requests. It is recommended
  # that you fix these values when running a production server!
  # If you're using threadsafe! mode, you probably don't want to set these values,
  # since 1 runtime(default for threadsafe mode) will be enough.
  # config.webxml.jruby.min.runtimes = 2
  # config.webxml.jruby.max.runtimes = 4

  # JNDI data source name
  # config.webxml.jndi = 'jdbc/rails'
end
