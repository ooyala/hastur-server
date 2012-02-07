#!/usr/bin/env ruby

require "test/unit"
require_relative "./integration_test_helper"

class RegisterTest < Test::Unit::TestCase

  def test_register
    # wait for 5 second for registration to happen
    sleep 5
    # get messages from the register shims
    sec_1_messages = Hastur::Test::ZMQ.all_payloads_to(:register)
    # verify that the messages on the register shims are registration messages
    sec_1_messages.each do |m|
      assert_equal("register_client", m['method'])
    end
    # verify that the count of messages on the notify shims are accurate
    assert_equal(2, sec_1_messages.size)
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
                   :name => :client2,
                   :command => "./bin/hastur-client.rb --router <%= zmq[:router] %> --port 8126",
                   # TODO(noah): mock UDP port to catch or forward messages?
                 },
                 {
                   :name => :router,
                   :command => <<EOS ,
    ./infrastructure/hastur-router.rb --heartbeat-uri <%= zmq[:heartbeat] %>
                     --register-uri <%= zmq[:register] %>
                     --notify-uri <%= zmq[:notify] %> --stats-uri <%= zmq[:stat] %>
                     --log-uri <%= zmq[:log] %> --error-uri <%= zmq[:error] %>
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
                       { :name => :errors, :type => :push, :listen => 4350 },
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
    ./tools/zmqcli.rb --type pull --connect --prefix [errors] --uri <%= zmq[:errors] %>
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
    `pkill -f hastur-cient`
    `pkill -f hastur-router`
    `pkill -f zmqcli`
    Hastur::Test::ZMQ.reset
    puts "Topology is torn down..."
  end
end

