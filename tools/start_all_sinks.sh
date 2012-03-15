#!/bin/bash

./zmqcli.rb --type pull --connect --prefix [stats] --uri tcp://127.0.0.1:8127 &
./zmqcli.rb --type pull --connect --prefix [events] --uri tcp://127.0.0.1:8128 &
./zmqcli.rb --type pull --connect --prefix [log] --uri tcp://127.0.0.1:8129 &
./zmqcli.rb --type pull --connect --prefix [error] --uri tcp://127.0.0.1:8130 &
./zmqcli.rb --type pull --connect --prefix [rawdata] --uri tcp://127.0.0.1:8131 &
./zmqcli.rb --type pull --connect --prefix [heartbeat] --uri tcp://127.0.0.1:8132 &
./zmqcli.rb --type pull --connect --prefix [registration] --uri tcp://127.0.0.1:8133 &






