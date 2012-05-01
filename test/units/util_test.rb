require_relative "../test_helper"
require "hastur-server/util"

class UtilTest < Scope::TestCase
  include Hastur::Util

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
  end

  context "to_valid_zmq_uri" do
    should "Allow valid URIs" do
      # Check 0.0.0.0, * and localhost
      assert_equal "ipc://127.0.0.1", to_valid_zmq_uri("ipc://localhost")
      assert_equal "tcp://0.0.0.0:8888", to_valid_zmq_uri("tcp://*:8888")
      assert_equal "ipc://0.0.0.0:4999", to_valid_zmq_uri("ipc://0.0.0.0:4999")
      assert_equal "tcp://0.0.0.0:4999", to_valid_zmq_uri("tcp://0.0.0.0:4999")

      # Test unmodified URIs of various forms
      assert_equal "inproc://1.2.3.4:1777", to_valid_zmq_uri("inproc://1.2.3.4:1777")
      assert_equal "tcp://subdomain.bob.co.uk:64234", to_valid_zmq_uri("tcp://subdomain.bob.co.uk:64234")
      assert_equal "tcp://bob.com", to_valid_zmq_uri("tcp://bob.com")
    end

    should "Reject invalid URIs" do
      assert_raises RuntimeError do
        to_valid_zmq_uri "trans:/bob.com/a_path"
      end

      assert_raises RuntimeError do
        to_valid_zmq_uri "trans://bob.com:abcd/non_numeric_port"
      end

      assert_raises RuntimeError do
        to_valid_zmq_uri "trans:///hostless_path"
      end

      assert_raises RuntimeError do
        to_valid_zmq_uri "trans://"
      end

      assert_raises RuntimeError do
        to_valid_zmq_uri "epgm://0.0.0.0:8888/epgm_not_allowed"
      end

      assert_raises RuntimeError do
        to_valid_zmq_uri "epgm://0.0.0.0:8888"
      end
    end

    should "Reject bad transports" do
      assert_raises RuntimeError do
        to_valid_zmq_uri "tr://bob.com/some_path"
      end

      assert_raises RuntimeError do
        to_valid_zmq_uri "too_long://bob.com/some_path"
      end
    end
  end

  context "connect" do
    should "allow multiple connect on a new socket" do
      ctx = mock("ZMQ context")
      socket = mock("ZMQ socket")
      ctx.expects(:socket).returns(socket)

      url1 = "tcp://host1:1234"
      url2 = "tcp://host2:5678"
      url3 = "tcp://host3.com"

      socket.stubs(:setsockopt).returns(0)
      socket.expects(:connect).with(url1).returns(0)
      socket.expects(:connect).with(url2).returns(0)
      socket.expects(:connect).with(url3).returns(0)

      connect_socket(ctx, :pull, [ url1, url2, url3 ])
    end
  end

  context "send and receive" do
    setup do
      @socket = mock "socket"
      @strings = [ "a", "b", "c" ]
      @msgs = [ Object.new, Object.new, Object.new ]  # Random objs for testing
      Hastur::Util.hastur_logger.stubs(:error)
    end

    should "fail correctly when ZMQ send_strings fails" do
      @socket.expects(:send_strings).with(@strings).returns(-1)
      assert_equal false, Hastur::Util.send_strings(@socket, @strings)
    end

    should "fail correctly when ZMQ send_msgs fails" do
      @socket.expects(:sendmsgs).with(@msgs).returns(-1)
      assert_equal false, Hastur::Util.send_msgs(@socket, @msgs)
    end

    should "succeed correctly when ZMQ send_strings succeeds" do
      @socket.expects(:send_strings).with(@strings).returns(0)
      assert_equal true, Hastur::Util.send_strings(@socket, @strings)
    end

    should "succeed correctly when ZMQ send_msgs succeeds" do
      @socket.expects(:sendmsgs).with(@msgs).returns(0)
      assert_equal true, Hastur::Util.send_msgs(@socket, @msgs)
    end
  end
end
