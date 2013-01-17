#!/bin/bash

# Run as root, or with "sudo ./install_server.sh"

# Install or upgrade Hastur agent
curl http://apt.us-east-1.ooyala.com/hastur.sh |sudo bash -s

if ! [ -f /etc/uuid ]
  then uuidgen > /etc/uuid
fi

mkdir -p /opt/hastur-server
cp ./*.?ar /opt/hastur-server/
cp ./start* /opt/hastur-server/
chmod +x /opt/hastur-server/start*
cp *.conf /etc/init/

# To get the job to re-read the config file, stop and start (not restart)
stop hastur-core
stop hastur-retrieval
start hastur-core
start hastur-retrieval
