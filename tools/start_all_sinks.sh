#!/bin/bash

# Start up one each of every type of sink (a.k.a. worker or server) that the router routes packets to,
# on the default port the router looks for.

./zmqcli.rb --type pull --connect --prefix [register] --uri tcp://127.0.0.1:8136 &
./zmqcli.rb --type pull --connect --prefix [notify] --uri tcp://127.0.0.1:8132 &
./zmqcli.rb --type pull --connect --prefix [heartbeat] --uri tcp://127.0.0.1:8133 &
./zmqcli.rb --type pull --connect --prefix [logs] --uri tcp://127.0.0.1:8128 &
./zmqcli.rb --type pull --connect --prefix [errors] --uri tcp://127.0.0.1:8130 &

./zmqcli.rb --type pull --connect --prefix [stats] --uri tcp://127.0.0.1:8127 &
#./zmqcli.rb --type pull --connect --prefix [stats] --uri tcp://127.0.0.1:4332 --spark true
