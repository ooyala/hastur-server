#!/bin/bash

./zmqcli.rb --type dealer --connect --send --uri tcp://127.0.0.1:4321 --prefix [register-agent] --infile ./register.json --route
