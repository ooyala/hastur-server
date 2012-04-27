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
      @filter = { :a => "b" }
    end

    should "reject the empty message" do
      assert_equal false, @syndicator.apply_one_filter(@filter, {}),
        "The empty message must not match { :a => 'b' }!"
    end

    should "reject the wrong value" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => "c" }),
        "{ :a => 'c' } must not match { 'a' => 'b' }!"
    end

    should "allow the right value" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "The filter must match the same hash"
    end

    should "allow symbol message keys" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { :a => "b" }),
        "The filter must match symbol message keys"
    end

    should "not allow symbol message values" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { :a => :b }),
        "The filter must not match symbol message values"
    end
  end

  context "checking that a key is present" do
    setup do
      @filter = { :a => true }
    end

    should "match a hash with that value" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "{ :a => true } must match a hash with the key set"
    end

    should "match a hash with that value set to false" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "a" => false }),
        "{ :a => true } must match a hash with the key set to false"
    end

    should "match a hash with that value set to nil" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "a" => nil }),
        "{ :a => true } must match a hash with the key set to nil"
    end

    should "not match a hash without that value" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "c" => "b" }),
        "{ :a => true } must not match a hash without that key set"
    end

    should "not match the empty hash" do
      assert_equal false, @syndicator.apply_one_filter(@filter, {}),
        "{ :a => true } must not match the empty hash"
    end
  end

  context "checking that a key is absent" do
    setup do
      @filter = { :a => false }
    end

    should "not match a hash with that value" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "{ :a => false } must not match a hash with the key set"
    end

    should "not match a hash with that value set to false" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => false }),
        "{ :a => false } must match a hash with the key set to false"
    end

    should "not match a hash with that value set to nil" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => nil }),
        "{ :a => false } must match a hash with the key set to nil"
    end

    should "match a hash without that value" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "c" => "b" }),
        "{ :a => false } must not match a hash without that key set"
    end

    should "match the empty hash" do
      assert_equal true, @syndicator.apply_one_filter(@filter, {}),
        "{ :a => false } must not match the empty hash"
    end
  end

  context "checking that a label is present" do
    setup do
      @filter = { :labels => { :a => true } }
    end

    should "match a hash with that label" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => "b" } }),
        "{ :labels => { :a => true } } must match a hash with the label set"
    end

    should "match a hash with that label set to false" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => false } }),
        "{ :labels => { :a => true } } must match a hash with the label set to false"
    end

    should "not match a hash with no labels" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "{ :labels => { :a => true } } must not match a hash with no labels"
    end

    should "match a hash with that label set to nil" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => nil } }),
        "{ :labels => { :a => true } } must match a hash with the label set to nil"
    end

    should "not match a hash without that label" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "c" => "b" } }),
        "{ :labels => { :a => true } } must not match a hash without that label set"
    end

    should "not match the empty hash" do
      assert_equal false, @syndicator.apply_one_filter(@filter, {}),
        "{ :labels => { :a => true } } must not match the empty hash"
    end
  end

  context "checking that a label is absent" do
    setup do
      @filter = { :labels => { :a => false } }
    end

    should "not match a hash with that label" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => "b" } }),
        "{ :labels => { :a => false } } must not match a hash with the label set"
    end

    should "not match a hash with that label set to false" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => false } }),
        "{ :labels => { :a => false } } must not match a hash with the label set to false"
    end

    should "match a hash with no labels" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "{ :labels => { :a => false } } must match a hash with no labels"
    end

    should "not match a hash with that label set to nil" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => nil } }),
        "{ :labels => { :a => false } } must not match a hash with the label set to nil"
    end

    should "match a hash without that label" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "labels" => { "c" => "b" } }),
        "{ :labels => { :a => false } } must match a hash without that label set"
    end

    should "match the empty hash" do
      assert_equal true, @syndicator.apply_one_filter(@filter, {}),
        "{ :labels => { :a => false } } must match the empty hash"
    end
  end

  context "checking a label's value" do
    setup do
      @filter = { :labels => { :a => "b" } }
    end

    should "match a hash with that label" do
      assert_equal true, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => "b" } }),
        "{ :labels => { :a => \"b\" } } must match a hash with the label set"
    end

    should "not match a hash with that label set to false" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => false } }),
        "{ :labels => { :a => \"b\" } } must not match a hash with the label set to false"
    end

    should "not match a hash with no labels" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "a" => "b" }),
        "{ :labels => { :a => \"b\" } } must not match a hash with no labels"
    end

    should "not match a hash with that label set to nil" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "a" => nil } }),
        "{ :labels => { :a => \"b\" } } must not match a hash with the label set to nil"
    end

    should "not match a hash without that label" do
      assert_equal false, @syndicator.apply_one_filter(@filter, { "labels" => { "c" => "b" } }),
        "{ :labels => { :a => \"b\" } } must not match a hash without that label set"
    end

    should "not match the empty hash" do
      assert_equal false, @syndicator.apply_one_filter(@filter, {}),
        "{ :labels => { :a => \"b\" } } must not match the empty hash"
    end
  end

end
