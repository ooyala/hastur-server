require "json"
require "test/unit"
require_relative "../hastur_client"
require_relative "lib/mock"

class TestRegisterClient < Test::Unit::TestCase
  def setup
    # start hastur_client
    @client = HasturClient.new
    @client.start
    # set up the router
    @router = Hastur::Mock::Router.new
  end

  def teardown
    # TODO(viet): figure out how to gracefully exit this netcat command. Seems like
    #             the connection is being kept open indefinitely
    system "kill -9 `ps -ef | grep 'nc -u 127.0.0.1 8125' | grep -v grep | awk {'print $2'}`"
    sleep 1
    @client.stop
    @router.unbind unless @router.nil?
  end

  def test_register_client
    msg = nil
    # listen for a register-client message
    msgs = @router.recv_multipart
    begin
      hash = JSON.parse(msgs[-1])
      # ensure that method is correct
      assert_equal(hash['method'], "register_client")
      # ensure that name is not nil
      assert_not_nil(hash['params']['name'])
      # ensure that uuid exists
      assert_equal(hash['params']['name'].size, 36)
      # ensure that the topic is set correctly in the multipart message
      assert_equal(msgs[-2], "register")
    rescue Exception => e
      assert(false, e.message)
    end
  end
end
