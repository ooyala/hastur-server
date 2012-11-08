require "bundler/gem_tasks"
require "rake/testtask"

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
