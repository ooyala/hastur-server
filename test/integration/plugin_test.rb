#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"

class PluginTest < Test::Unit::TestCase

  def test_plugin
    # let the schedule message actually get through
    sleep 10
    # get messages from the sink shims
    messages = Hastur::Test::ZMQ.all_payloads_to(:stats)
    # verify that the messages on the heartbeat shims are heartbeat messages
    assert_equal(messages.size, messages.fuzzy_filter( {"method" => "stats"} ).size)
    # verify that the count of messages on the heartbeat shims are accurate
    assert_equal(2, messages.size)
  end

  def setup
    Dir.chdir HASTUR_ROOT
    processes = [
                 {
                   :name => :client1,
                   :command => "./bin/hastur-client.rb --router <%= zmq[:router] %> --port 8125 --uuid thisismyuuid",
                   # TODO(noah): mock UDP port to catch or forward messages?
                 },
                 {
                   :name => :router,
                   :command => <<EOS ,
    ./infrastructure/hastur-router.rb --heartbeat-uri <%= zmq[:heartbeat] %>
                     --register-uri <%= zmq[:register] %>
                     --notify-uri <%= zmq[:notify] %> --stats-uri <%= zmq[:stats] %>
                     --logs-uri <%= zmq[:logs] %> --error-uri <%= zmq[:errors] %>
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
    ./tools/zmqcli.rb --type pull --connect --prefix [notify] --uri <%= zmq[:notify] %>
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
                 {
                   :name => :scheduleD,
                   :command => <<EOS ,
    ./infrastructure/hastur-scheduler.rb --initial-sleep 5 --router <%= zmq[:router] %> --data test/data/json/sample.txt --client thisismyuuid
EOS
                   # TODO(noah): mock UDP port to catch or forward messages?
                 }
    ]

    @topology = Hastur::Test::Topology.new(processes)
    puts "Starting up all of the topology components..."
    @topology.start_all
    puts "Started up all of the topology components..."
  end

  def teardown
    puts "Tearing down all of the topology components..."
    @topology.stop_all
    `pkill -f hastur-client`
    `pkill -f hastur-router`
    `pkill -f zmqcli`
    Hastur::Test::ZMQ.reset
    puts "Topology is torn down..."
  end
end

