#!/usr/bin/env ruby

require 'rubygems'
require 'minitest/autorun'
require_relative '../lib/hastur/input/json'

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
    stat = Hastur::Input::JSON.decode("{\"foo\": \"bar\"}")
    refute_nil stat, "should not return nil when fed valid JSON"

    stat = Hastur::Input::JSON.decode("{globs:1|c")
    assert_nil stat, "should return nil when fed invalid JSON"

    stat = Hastur::Input::JSON.decode("{globs:1|c")
    assert_nil stat, "should return nil when fed invalid JSON"
  end
end

