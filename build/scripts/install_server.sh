#!/bin/bash

mkdir -p /opt/hastur-server
cp ./*.?ar /opt/hastur-server/
cp ./start* /opt/hastur-server/
cp *.conf /etc/init/

# Upstart
start /etc/init/hastur-core.conf
start /etc/init/hastur-retrieval.conf
