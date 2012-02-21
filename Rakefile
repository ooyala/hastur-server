require "bundler/gem_tasks"
require "rake/testtask"

namespace "test" do
  desc "Unit tests for Hastur"
  Rake::TestTask.new(:units) do |t|
    t.libs += ["test"]  # require from test subdir
    t.test_files = Dir["test/units/**/*_test.rb", "client_apis/ruby/test/*test.rb"]
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

  task :integrations => integration_tests.map { |t| "test:integration:#{t}" }
end

# Put together a test target for Jenkins
task :test => ["test:units", "test:integrations"] do
  puts "All tests completed..."
end
