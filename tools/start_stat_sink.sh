#!/bin/bash

./zmqcli.rb --type pull --connect --prefix [stats] --uri tcp://127.0.0.1:4332 --spark true
