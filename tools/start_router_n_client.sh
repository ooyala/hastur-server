#!/bin/bash

../infrastructure/hastur-router.rb &
../bin/hastur-client.rb --router tcp://127.0.0.1:4321 --port 8125 --uuid thisismyuuid
