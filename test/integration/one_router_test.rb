#!/usr/bin/env ruby

require_relative "./integration_test_helper"

Dir.chdir HASTUR_ROOT

PROCESSES = [
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
                 --notify-uri <%= zmq[:notify] %> --stat-uri <%= zmq[:stat] %>
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
                   { :name => :pub, :type => :pub, :listen => 4322 },
                   { :name => :from_sink, :type => :pull, :listen => 4323 }
                 ],
               }
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

TOPOLOGY = Hastur::Test::Topology.new(PROCESSES)
TOPOLOGY.start_all
puts "Sleeping for 10 seconds..."
sleep 10

puts "Stopping all nodes."
TOPOLOGY.stop_all

STDERR.puts "********** Packet recipients: #{Hastur::Test::ZMQ.all_packet_receivers.inspect}"

#assert_equal 1, packets_to("client1").filter("method" => "heartbeat", "value" => /37$/).map {|p| p[:subpart]}.filter(:subfield => 7).count

puts "Done!"

`pkill -f hastur`
