require "test_helper"
require "hastur/zmq_utils"

class ZmqUtilsTest < Scope::TestCase
  include Hastur::ZMQUtils

  context "checking URI validity" do
    should "reject hostname with no protocol" do
      assert_raises(RuntimeError) do
        check_uri "yoyodyne.com:4999"
      end
    end

    should "reject hostname without host" do
      assert_raises(RuntimeError) do
        check_uri "https:///whatever"
      end
    end

    should "allow hostname with port" do
      check_uri "https://jim-bob.com:4791"
    end

    # Right now, URIs with paths are disallowed.
    # That's fine, given how we're using them.

    context "with ZMQ version 2" do
      setup do
        ZMQ::LibZMQ.stubs("version2?".to_sym).returns(true)
      end

      should "reject a hostname based on localhost" do
        assert_raises(RuntimeError) do
          check_uri "zmq://localhost:174"
        end
      end
    end

    # TODO(noah): make sure we allow localhost with ZMQ version 3.X
  end
end
