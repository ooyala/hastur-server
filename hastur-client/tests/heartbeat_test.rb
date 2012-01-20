require "json"
require "test/unit"
require_relative "../hastur_client"
require_relative "lib/mock"

class TestHeartbeat < Test::Unit::TestCase
  def setup
    # start hastur_client
    client = HasturClient.new
    client.start
    # set up the router
    msg = nil
    @router = Hastur::Mock::Router.new
  end

  def teardown

  end

  def test_heartbeat
    heartbeat_count = 0
    t = Thread.start do
      msgs = []
      loop do
        begin
          # listen for heartbeat messages
          msgs = @router.recv_multipart
          hash = JSON.parse(msgs[-1])
          STDERR.puts hash
          if hash['method'] == 'heartbeat'
            heartbeat_count = heartbeat_count + 1
          end
        rescue Exception => e

        end
      end
    end

    sleep 35

    Thread.kill(t)

    assert_equal(2, heartbeat_count)
  end
end
