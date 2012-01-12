require "rubygems"
require "bundler"
Bundler.require(:default, :development)
require "minitest/autorun"

# For testing Hastur-mq itself, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")

require "hastur-mq"
#require "ecology/test_methods"

class Scope::TestCase
  #include Ecology::Test

  # Add test methods here
end
