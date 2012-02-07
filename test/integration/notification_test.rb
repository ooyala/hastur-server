#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"

class NotificationTest < Test::Unit::TestCase

  def test_notification
    # send a notification
    sleep 1
    puts "Sending notification..."
    u = UDPSocket.new
    u.send('{ "params":{ "name":"name", "subsystem":"backlot", "uuid":"thisismyuuid", "id":"notification-id"}, "method":"notify" }', 
           0, "127.0.0.1", 8125)
    # wait for 1 second for notification
    sleep 1
    # get messages from the sink shims for notifications
    sec_1_messages = Hastur::Test::ZMQ.all_payloads_to(:notify)
    # wait for another 5 seconds for a re-notification
    sleep 6
    # get messages from the sink shims for notifications
    sec_6_messages = Hastur::Test::ZMQ.all_payloads_to(:notify)
    # verify that the messages on the notify shims are notify messages
    assert_equal(sec_1_messages.size, sec_1_messages.fuzzy_filter( {"method" => "notify"} ).size)
    assert_equal(sec_6_messages.size, sec_6_messages.fuzzy_filter( {"method" => "notify"} ).size)
    # verify that the count of messages on the notify shims are accurate
    assert_equal(1, sec_1_messages.size)
    assert_equal(2, sec_6_messages.size)
    # TODO(viet): whenever there is a true notificationD, need to use that to test notification_acks
  end

  def setup
    Dir.chdir HASTUR_ROOT
    processes = [
                 {
                   :name => :client1,
                   :command => "./bin/hastur-client.rb --router <%= zmq[:router] %> --port 8125",
                   # TODO(noah): mock UDP port to catch or forward messages?
                 },
                 {
                   :name => :router,
                   :command => <<EOS ,
    ./infrastructure/hastur-router.rb --heartbeat-uri <%= zmq[:heartbeat] %>
                     --register-uri <%= zmq[:register] %>
                     --notify-uri <%= zmq[:notify] %> --stats-uri <%= zmq[:stats] %>
                     --logs-uri <%= zmq[:logs] %> --error-uri <%= zmq[:error] %>
                     --router-uri <%= zmq[:router] %> --from-sink-uri <%= zmq[:from_sink] %>
EOS
                   :resources => {
                     :zmq => [
                       { :name => :router, :type => :router, :listen => 4321 },
                       { :name => :register, :type => :push, :listen => 4330 },
                       { :name => :notify, :type => :push, :listen => 4331 },
                       { :name => :stats, :type => :push, :listen => 4332 },
                       { :name => :heartbeat, :type => :push, :listen => 4333 },
                       { :name => :logs, :type => :push, :listen => 4334 },
                       { :name => :error, :type => :push, :listen => 4350 },
#                       { :name => :pub, :type => :pub, :listen => 4322 },
                       { :name => :from_sink, :type => :pull, :listen => 4323 }
                     ],
                   }
                 },
                 {
                   :name => :register_worker,
                   :command => <<EOS ,
    ./tools/zmqcli.rb --type pull --connect --prefix [register] --uri <%= zmq[:register] %>
EOS
                 },
                 {
                   :name => :notify_worker,
                   :command => <<EOS ,
    ./tools/zmqcli.rb --color green --precolor blue --type pull --connect --prefix [notify] --uri <%= zmq[:notify] %>
EOS
                 },
                 {
                   :name => :stats_worker,
                   :command => <<EOS ,
    ./tools/zmqcli.rb --type pull --connect --prefix [stats] --uri <%= zmq[:stats] %>
EOS
                 },
                 {
                   :name => :heartbeat_worker,
                   :command => <<EOS ,
    ./tools/zmqcli.rb --type pull --connect --prefix [heartbeat] --uri <%= zmq[:heartbeat] %>
EOS
                 },
                 {
                   :name => :logs_worker,
                   :command => <<EOS ,
    ./tools/zmqcli.rb --type pull --connect --prefix [logs] --uri <%= zmq[:logs] %>
EOS
                 },
                 {
                   :name => :errors_worker,
                   :command => <<EOS ,
    ./tools/zmqcli.rb --type pull --connect --prefix [error] --uri <%= zmq[:error] %>
EOS
                 },
    ]

    @topology = Hastur::Test::Topology.new(processes)
    puts "Starting up all of the topology components..."
    @topology.start_all
    puts "Started up all of the topology components..."
  end

  def teardown
    puts "Tearing down all of the topology components..."
    @topology.stop_all
    `pkill -f hastur`
    @topology.reset
    puts "Topology is torn down..."
  end
end

