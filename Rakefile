require "bundler/gem_tasks"
require "rake/testtask"
require "warbler"

# used in build/tasks/package.rake and below
PROJECT_TOP = Rake.application.find_rakefile_location[1]
PROJECT_DIR = File.basename(PROJECT_TOP)

Dir["build/tasks/*.rake"].each { |task| load task }

namespace "test" do
  desc "Unit tests for Hastur"
  Rake::TestTask.new(:units) do |t|
    t.libs += ["test"]  # require from test subdir
    t.test_files = Dir["test/units/**/*_test.rb"]
    t.verbose = true
  end

  desc "Run all integration tests"
  task :integrations do
    puts "(Integration tests!)"
  end

  integration_tests = []

  namespace "integration" do
    Dir["test/integration/*_test.rb"].each do |file|
      test_name = file.sub(/_test.rb$/, "").sub(/^test\/integration\//, "")
      integration_tests << test_name
      Rake::TestTask.new(test_name.to_sym) do |t|
        t.libs += [".", "test"]  # require from test subdir
        t.test_files = [file]
        t.verbose = true
      end
    end
  end

  namespace "units" do
    desc "Long running unit tests with timeouts"
    Rake::TestTask.new(:long) do |t|
      t.libs += ["test"]
      t.test_files = Dir["test/long/units/**/*_test.rb"]
      t.verbose = true
    end

    desc "Runs all of the unit tests"
    task :full => ["test:units", "test:units:long"] do
      puts "All unit tests completed..."
    end
  end

  shameful_integration_tests = integration_tests

  LIST_OF_SHAME = [ 'bring_router_down', 'bring_down', 'bring_sink_down', 'bring_up', 'plugin_registration', 'event', 'plugin', 'heartbeat', 'core_router' ]

  unless LIST_OF_SHAME.nil? || LIST_OF_SHAME.empty?
    puts "****************************************************"
    puts "CURRENT LIST OF SHAME: #{LIST_OF_SHAME.join(", ")}"
    puts "****************************************************"

    integration_tests -= LIST_OF_SHAME
  end

  task :integrations => integration_tests.map { |t| "test:integration:#{t}" }
  desc "Tests including the LIST_OF_SHAME"
  task :shameful => shameful_integration_tests.map { |t| "test:integration:#{t}" }
end

# Put together a test target for Jenkins
task :test => ["test:units", "test:integrations"] do
  puts "All tests completed..."
end

#
# Warbler tasks to package for JRuby deploy
#

task :delete_retrieval_war do
  File.unlink "retrieval_v2.war" if File.exist?("retrieval_v2.war")
end

Warbler::Task.new("retrieval_war", Warbler::Config.new do |config|
  require "jruby_astyanax-jars"

  config.jar_name = "retrieval_v2"
  config.features = ["executable"]

  # See config/warble.rb for explanation of config variables
  config.dirs = %w(lib tools)
  config.excludes = FileList["**/*~"]
  # TODO(noah): Can we remove this and just use the Astyanax jars gem directly?
  config.java_libs += FileList[File.join JRUBY_ASTYANAX_JARS_HOME, "*.jar"]
  config.java_libs += ["lib/hastur-server/native/native_code.jar"]
  config.bundler = false  # This doesn't seem to turn off the gemspec
  config.gem_dependencies = true
  config.webserver = 'jetty'
  config.webxml.booter = :rack
  config.webxml.jruby.compat.version = "1.9"
  config.webxml.rackup = File.read("config_v2.ru")
end)
# Workaround for Warbler bug (https://github.com/jruby/warbler/issues/86)
task :retrieval_war => :delete_retrieval_war
task :retrieval_war => :native_jar

Warbler::Task.new("core_jar", Warbler::Config.new do |config|
  config.jar_name = "core"

  # See config/warble.rb for explanation of config variables
  config.dirs = %w(lib vendor tools)
  config.excludes = FileList["**/*~"]
  # TODO(noah): Can we remove this and just use the Astyanax jars gem directly?
  config.java_libs += FileList[File.join JRUBY_ASTYANAX_JARS_HOME, "*.jar"]
  config.bundler = false  # This doesn't seem to turn off the gemspec
  config.gem_dependencies = false
end)

# Eventually this will get really slow and I'll have to do it in a more
# reasonable way.
task :native_jar do
  Dir.chdir File.join(File.dirname(__FILE__), "lib", "hastur-server", "native")

  Dir["**/*.class"].each { |f| File.unlink f }

  # TODO(noah): support java/scala files not in top directory

  unless Dir["*.scala"].empty?
    system "scalac *.scala"
    unless $?.success?
      raise "Couldn't compile scala files!"
    end
  end

  unless Dir["*.java"].empty?
    system "javac *.java"
    unless $?.success?
      raise "Couldn't compile java files!"
    end
  end

  system "jar -cf native_code.jar `find . -name '*.class\'`"
  unless $?.success?
    raise "Couldn't archive java/scala class files to jar!"
  end

  Dir.chdir File.dirname(__FILE__)
end

#
# undesirable but useful hacks follow ...
#

desc "Pushes the local code to hastur-core-dev1.us-east-1.ooyala.com"
task :push_dev do
  system "rsync -ave ssh #{PROJECT_TOP} hastur-core-dev1.us-east-1.ooyala.com:"
  system "ssh hastur-core-dev1.us-east-1.ooyala.com '(cd #{PROJECT_DIR} ; tools/restart_dev_unicorn.sh)'"
end

desc "tail your logfile on hastur-core-dev1.us-east-1.ooyala.com"
task :tail_dev do
  system "ssh hastur-core-dev1.us-east-1.ooyala.com 'tail -f unicorn-*.log'"
end

# quick test - pull stats for Spaceghost
desc "push / tail / curl"
task :test_dev => :push_dev do
  sleep 1
  system "curl -m 30 -H \"Accept: application/json\" http://ec2-107-22-157-160.compute-1.amazonaws.com:8888/nodes/6bbaffa0-7140-012f-1b93-001e6713f84b/stats &"
  system "ssh hastur-core-dev1.us-east-1.ooyala.com 'tail -f unicorn-*.log'"
end

task :evil_deploy => ["build"] do
  system "cl-sendfile.pl --list hastur -l pkg/hastur-server-#{Hastur::SERVER_VERSION}.gem -r #{ENV['HOME']}"
  system "cl-run.pl --list hastur -c 'sudo /opt/hastur/bin/gem install --local --no-ri --no-rdoc ~/hastur-server-#{Hastur::SERVER_VERSION}.gem'"
end
