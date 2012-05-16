require_relative "../test_helper"
require "hastur-server/trigger/state_handler"

class StateHandlerTest < Scope::TestCase
  context "state handler with working cassandra" do
    setup do
      @cf = Hastur::Trigger::DEFAULT_CF
      @col = Hastur::Trigger::DEFAULT_COL
      @file = mock "file"
      @key = mock "key"
      @client = mock "cass client"
      Digest::MD5.expects(:hexdigest).with(@file).returns @key
      ::Cassandra.expects(:new).returns @client
      @handler = Hastur::Trigger::StateHandler.new @file
    end

    should "set state properly" do
      hash = mock "state hash"
      hash.expects(:is_a?).returns(Hash)
      json = mock "json dump of hash"
      MultiJson.expects(:dump).with(hash).returns json
      @client.expects(:insert).with(@cf, @key, { @col => json }, {})
      @handler.set_state hash
    end

    should "get state properly" do
      val = mock "cassandra val"
      hash = mock "hash from val"
      @client.expects(:get).with(@cf, @key, @col, {}).returns val
      MultiJson.expects(:load).with(val).returns hash
      assert_equal hash, @handler.get_state
    end
  end
end
