#!/bin/bash

../bin/hastur-router.rb --uuid 11111111-2222-3333-4444-555555555555 &
../bin/hastur-agent.rb --router tcp://127.0.0.1:8126 --port 8125 --uuid ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb