#!/bin/bash
pkill -f ./worker.rb
pkill -f ./worker.rb
pkill -f mongrel2
pkill -f mongrel2
./worker.rb &
m2sh start -db config.sqlite -host localhost &
