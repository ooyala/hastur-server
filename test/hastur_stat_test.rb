#!/usr/bin/env ruby

require 'rubygems'
require 'minitest/autorun'
require_relative '../lib/hastur/stat'

class TestClassHasturStat < MiniTest::Unit::TestCase
  def test_basic_new
    now = Time.new

    s = Hastur::Stat.new(
      :name      => "foo.bar",
      :value     => 1024,
      :units     => "s",
      :timestamp => now.to_f,
      :tags      => { :blahblah => 456 }
    )

    refute_nil s, "Hastur::Stat.new"
    assert_kind_of Hastur::Stat, s, "Hastur::Stat.new"

    assert_kind_of String, s.name     # always a String
    assert_kind_of String, s.units    # always a String
    assert_kind_of Hash, s.tags       # always a Hash (when defined)
    refute_nil s.value
    refute_nil s.timestamp

    assert_equal "foo.bar", s.name
    assert_equal 1024, s.value
    assert_equal "s", s.units
    assert_equal now.to_f, s.timestamp
  end
end

