# This must be first, before anything else
require_relative "./hastur_simplecov"

require "rubygems"
require "bundler"
#Bundler.require(:default, :development)
require "minitest/autorun"
require "scope"
require "mocha"

# Require mandatory jars
require "java"
Dir["build/include_jars/*.jar"].each { |f| require f }

# For testing Hastur components, use the local version *first*.
$LOAD_PATH.unshift File.join(File.dirname(__FILE__), "..", "lib")

# Easy-to-spot fake UUIDs
A1UUID = '11111111-2222-3333-4444-555555555555'
A2UUID = 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'
A3UUID = '66666666-7777-8888-9999-aaaaaaaaaaaa'

class Scope::TestCase
  #include Ecology::Test

  # Hastur unit test methods go here
end
