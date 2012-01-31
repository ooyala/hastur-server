#!/usr/bin/env ruby

require "../topology_helper"

PROCESSES = [
             {
               :name => :client1,
               :command => "./hastur-client.rb --router <%= zmq[:router] %> --port 8125",
               # TODO(noah): mock UDP port to catch or forward messages?
             },
             {
               :name => :client2,
               :command => "./hastur-client.rb --router <%= zmq[:router] %> --port 8126",
               # TODO(noah): mock UDP port to catch or forward messages?
             },
             {
               :name => :router,
               :command => <<EOS ,
hastur-router.rb --heartbeat-uri <%= zmq[:heartbeat] %> --register-uri <%= zmq[:register] %>
                 --notify-uri <%= zmq[:notify] %> --stat-uri <%= zmq[:stat] %>
                 --log-uri <%= zmq[:log] %> --error-uri <%= zmq[:error] %>
                 --router-uri <%= zmq[:router] %> --from-sink-uri <%= zmq[:from_sink] %>
EOS
               :resources => {
                 :zmq => [
                   { :name => :router, :type => :router, :listen => 4321 },
                   { :name => :register, :type => :push, :listen => 4330 }
                   { :name => :notify, :type => :push, :listen => 4331 }
                   { :name => :stat, :type => :push, :listen => 4332 }
                   { :name => :heartbeat, :type => :push, :listen => 4333 }
                   { :name => :logs, :type => :push, :listen => 4334 }
                   { :name => :error, :type => :push, :listen => 4350 }
                   { :name => :pub, :type => :pub, :listen => 4322 }
                   { :name => :from_sink, :type => :pull, :listen => 4323 }
                 ],
               }
             },
             {
               :name => :notify_worker,
               :command => <<EOS ,
zmqcli.rb --type pull --connect --prefix [notify] --uri <%= zmq[:notify] %>
EOS
             },
             {
               :name => :stats_worker,
               :command => <<EOS ,
zmqcli.rb --type pull --connect --prefix [stats] --uri <%= zmq[:stats] %>
EOS
             },
             {
               :name => :heartbeat_worker,
               :command => <<EOS ,
zmqcli.rb --type pull --connect --prefix [heartbeat] --uri <%= zmq[:heartbeat] %>
EOS
             },
             {
               :name => :logs_worker,
               :command => <<EOS ,
zmqcli.rb --type pull --connect --prefix [logs] --uri <%= zmq[:logs] %>
EOS
             },
             {
               :name => :errors_worker,
               :command => <<EOS ,
zmqcli.rb --type pull --connect --prefix [errors] --uri <%= zmq[:errors] %>
EOS
             },
]

topology = Hastur::Test::Topology.new PROCESSES
