# This must be first, before anything else
require_relative "./hastur_simplecov"

require "rubygems"
require "bundler"
#Bundler.require(:default, :development)
require "minitest/autorun"
require "scope"
require "mocha"

# For testing Hastur components, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")

class Scope::TestCase
  #include Ecology::Test

  # Hastur unit test methods go here
end
