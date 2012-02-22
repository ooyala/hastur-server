#!/bin/bash

#./zmqcli.rb --type pull --connect --prefix [stats] --uri tcp://127.0.0.1:8127 &
./zmqcli.rb --type pull --connect --prefix [log] --uri tcp://127.0.0.1:8128 &
./zmqcli.rb --type pull --connect --prefix [acks] --uri tcp://127.0.0.1:8129 &
./zmqcli.rb --type pull --connect --prefix [notify] --uri tcp://127.0.0.1:8132 &
./zmqcli.rb --type pull --connect --prefix [heartbeat] --uri tcp://127.0.0.1:8133 &
./zmqcli.rb --type pull --connect --prefix [register] --uri tcp://127.0.0.1:8136 &






