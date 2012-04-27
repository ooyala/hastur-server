require_relative "../test_helper"
require "hastur-server/syndicator"

class SyndicatorTest < Scope::TestCase
  setup do
    @syndicator = Hastur::Syndicator.new
  end

  context "empty filter" do
    setup do
      @filter = {}
    end

    should "allow the empty message" do
      assert_equal true, @syndicator.apply_one_filter(@filter, {})
    end
  end

  context "checking non-labels" do
    setup do
      @filter = { "a" => "b" }
    end

    should "reject the empty message" do
      assert_equal false, @syndicator.apply_one_filter(@filter, {}),
        "The empty message must not match { 'a' => 'b' }!"
    end

    should "reject the wrong value" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => "c" }),
        "{ 'a' => 'c' } must not match { 'a' => 'b' }!"
    end

    should "allow the right value" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "The filter must match the same hash"
    end
  end
end
