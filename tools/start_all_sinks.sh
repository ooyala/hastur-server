#!/bin/bash

# Start up one each of every type of sink (a.k.a. worker or server) that the router routes packets to,
# on the default port the router looks for.

./zmqcli.rb --type pull --connect --prefix [register] --uri tcp://127.0.0.1:4330 &
./zmqcli.rb --type pull --connect --prefix [notify] --uri tcp://127.0.0.1:4331 &
./zmqcli.rb --type pull --connect --prefix [stats] --uri tcp://127.0.0.1:4332 &
./zmqcli.rb --type pull --connect --prefix [heartbeat] --uri tcp://127.0.0.1:4333 &
./zmqcli.rb --type pull --connect --prefix [logs] --uri tcp://127.0.0.1:4334 &

./zmqcli.rb --type pull --connect --prefix [errors] --uri tcp://127.0.0.1:4350 &
