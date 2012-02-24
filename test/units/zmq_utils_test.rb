require "test_helper"
require "hastur-server/zmq_utils"

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

    should "allow multiple connect on a new socket" do
      ctx = mock("ZMQ context")
      socket = mock("ZMQ socket")
      ctx.expects(:socket).returns(socket)

      url1 = "http://host1:1234"
      url2 = "http://host2:5678"
      url3 = "http://host3.com"

      socket.stubs(:setsockopt)
      socket.expects(:connect).with(url1).returns(0)
      socket.expects(:connect).with(url2).returns(0)
      socket.expects(:connect).with(url3).returns(0)

      connect_socket(ctx, :pull, [ url1, url2, url3 ])
    end

  end
end
