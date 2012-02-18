#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'hastur/input/json'

class TestHasturInputJSON < MiniTest::Unit::TestCase
  def test_re_binary_garbage
    # early testing found random results from random binary data because I missed escaping a pipe
    # running at least 10 times should shake out accidental ascii in random data
    10.times do
      msg = open("/dev/urandom", "rb") do |io|
        io.read(1024)
      end
      stat = Hastur::Input::JSON.decode(msg)
      assert_nil stat, "RE should return nil when fed binary garbage"
    end
  end

  def test_json_doesnt_match
    stat = Hastur::Input::JSON.decode("{\"_route\": \"rawdata\", \"params\": {\"foo\": \"bar\"}}")
    refute_nil stat, "should not return nil when fed valid JSON with correct :method and :params"

    stat = Hastur::Input::JSON.decode("{\"foo\": \"bar\"}")
    assert_nil stat, "should return nil when fed valid JSON that does not have :method and :params"

    stat = Hastur::Input::JSON.decode("{globs:1|c")
    assert_nil stat, "should return nil when fed invalid JSON"

    stat = Hastur::Input::JSON.decode("{globs:1|c")
    assert_nil stat, "should return nil when fed invalid JSON"
  end

  def test_multi_line_json
    json =<<EOJSON

{
  "_route": "notification",
  "sla": 604800,
  "app": "dyson",
  "recipients": [
    "backlot-oncall",
    "backlot-fyi",
    "backlot-operations"
  ]
}

EOJSON

    assert_block do
      Hastur::Input::JSON.decode(json) rescue nil
    end
  end
end

