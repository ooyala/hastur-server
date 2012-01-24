require "json"
require "test/unit"
require_relative "../hastur_client"
require_relative "lib/mock"

class TestLogger < Test::Unit::TestCase
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

  def test_logs
    begin
      log_msg_found = false
      t = Thread.start do
        begin
          loop do
            msgs = @router.recv_multipart
            if msgs[-1].start_with?("Attempting to start up the client with uuid") && msgs[-2] == "log"
              log_msg_found = true
            end
          end
        rescue Exception => e
          puts e.backtrace.to_s
          assert(false)
        end
      end
      # read all of the message from the client
      sleep 5
      Thread.kill(t)
      assert_equal(true, log_msg_found)
    rescue JSON::ParserError => e
      # ignore
      puts "JSON #{e.message}"
    end
  end

  def test_errors
    # send a bad message to trigger an error message
    send_bad_message
    # read the message from hastur client
    begin
      log_msg_found = false
      t = Thread.start do
        begin
          loop do
            msgs = @router.recv_multipart
            if msgs[-1].start_with?("Unable to process message") && msgs[-2] == "error"
              log_msg_found = true
              idx = (msgs[-1] =~ /This message should make the hastur client throw error logs because it should be a JSON message/)
              log_msg_found &&= (idx != nil && idx > 0)
            end
          end
        rescue Exception => e
          puts e.message
          puts e.backtrace.to_s
        end
      end
      # read all of the message from the client
      sleep 5
      Thread.kill(t)
      assert_equal(true, log_msg_found)
    rescue JSON::ParserError => e
      # ignore
      puts "JSON #{e.message}"
    end
  end

  def send_bad_message
    # trigger a notification via netcat
    notification_msg = "This message should make the hastur client throw error logs because it should be a JSON message."
    f = IO.popen("echo '#{notification_msg}' | nc -u 127.0.0.1 8125")
  end
end
