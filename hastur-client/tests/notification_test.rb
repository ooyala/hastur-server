require "json"
require "test/unit"
require_relative "../hastur_client"
require_relative "lib/mock"

class TestNotification < Test::Unit::TestCase
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

  def test_notification
    # trigger a notification via netcat
    notification_msg = '{"params":{ "name" : "foo", "subsystem" : "fake", "uuid" : "thisismyfakeuuid"},"method":"notification"}' 
    input = JSON.parse(notification_msg)
    # put this in a thread because the nc command does not close.
    # TODO(viet): Need to figure this out
    m = Thread.start do
      `echo '#{notification_msg}' | nc -u 127.0.0.1 8125`
    end

    Thread.kill(m)
   
    match = false

    t = Thread.start do
      loop do
        begin
          # listen for messages
          msgs = @router.recv_multipart
          hash = JSON.parse(notification_msg)
          if hash['method'] == 'notification'
            if hash['params']['name'] == input['params']['name'] &&
                hash['params']['subsystem'] == input['params']['subsystem'] &&
                hash['params']['uuid'] == input['params']['uuid'] then
              match = true
            end
          end
        rescue Exception => e

        end
      end
    end

    sleep 2

    Thread.kill(t)

    assert(match)
  end
end
