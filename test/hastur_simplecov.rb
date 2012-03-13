if ENV["COVERAGE"] && !ENV["COVERAGE"].empty?
  require "simplecov"
  SimpleCov.start
end
