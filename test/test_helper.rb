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

# Require native code
begin
  require "hastur-server/native/native_code"
rescue
  raise "Please build native code using 'rake native_jar'!"
end

# Easy-to-spot fake UUIDs
A1UUID = '11111111-2222-3333-4444-555555555555'
A2UUID = 'ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb'
A3UUID = '66666666-7777-8888-9999-aaaaaaaaaaaa'

class Scope::TestCase
  #include Ecology::Test

  # Hastur unit test methods go here
end
