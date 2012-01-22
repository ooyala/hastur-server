require "json"
require "test/unit"
require_relative "../hastur_client"
require_relative "lib/mock"

class TestNotification < Test::Unit::TestCase
  def setup
    # start hastur_client
    @client = HasturClient.new
    @client.start
    # set up the router
    msg = nil
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

  def test_notification
    input = send_notification_message
    match = false
    t = Thread.start do
      loop do
        begin
          # listen for messages
          msgs = @router.recv_multipart
          hash = JSON.parse(msgs[-1])
          if hash['method'] == 'notification'
            if hash['params']['name'] == input['params']['name'] &&
                hash['params']['subsystem'] == input['params']['subsystem'] &&
                hash['params']['uuid'] == input['params']['uuid']
              match = true
            end
          end
        rescue JSON::ParserError => e
          # ignore for these messages that aren't JSON
        rescue Exception => e
          assert_fail
        end
      end
    end
    # let give 2 seconds for the message to go through and get process
    sleep 2
    # at this time, all messages should have processed
    Thread.kill(t)
    # ensure that the notification message was received
    assert(match, "Notification message was not received.")
  end

  def test_notification_ack
    input = send_notification_message
    notification_msg_count = 0
    t = Thread.start do
      loop do
        begin
          msgs = @router.recv_multipart
          hash = JSON.parse(msgs[-1])
          if hash['method'] == 'notification'
            if hash['params']['name'] == input['params']['name'] &&
                hash['params']['subsystem'] == input['params']['subsystem'] &&
                hash['params']['uuid'] == input['params']['uuid'] then
              notification_msg_count = notification_msg_count + 1
            end
          end
        rescue Exception => e
        end
      end
    end
    # sleep for 6 seconds to allow the notification and the resend of the notification to get through
    sleep 6
    assert_equal(2, notification_msg_count)
    # send notification_ack
    @router.send_msg('thisismyfakeuuid', ['notification_ack', '{ "id": "6e1ae900-2529-012f-1460-109addba6b5d" }'])
  end

  def send_notification_message
    # trigger a notification via netcat
    notification_msg = '{"params":{ "name" : "foo", "subsystem" : "fake", "uuid" : "thisismyfakeuuid", "id":"6e1ae900-2529-012f-1460-109addba6b5d"},"method":"notification"}' 
    input = JSON.parse(notification_msg)
    f = IO.popen("echo '#{notification_msg}' | nc -u 127.0.0.1 8125")
    input
  end
end
