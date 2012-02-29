#!/usr/bin/env ruby
$:.unshift File.join(File.dirname(__FILE__), '..', '..', 'lib')

require 'rubygems'
require 'minitest/autorun'
require 'hastur-server/libc_ffi'

class TestLibCFFI < MiniTest::Unit::TestCase
  def test_alarm
    assert_equal 0, LibC.alarm(5), "first alarm should return 0"
    remainder = LibC.alarm(0)
    assert remainder > 0, "second call to alarm should return > 0"
  end

  def test_getrusage
    ru = LibC.getrusage
    refute_nil ru
    assert_kind_of LibC::RUsage, ru
    assert ru[:ru_maxrss].kind_of? Fixnum
    assert ru[:ru_maxrss] > 1000
  end
end
