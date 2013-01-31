#!/bin/bash

# Run as root, or with "sudo ./install_server.sh"

MY_REGION=`which my-region`
if ! [ -f $MY_REGION ] && ! [ -f /bin/my-region ]
then
  echo "echo sv2" > /bin/my-region
  chmod ugo+rx /bin/my-region
fi

# Install or upgrade Hastur agent
curl http://apt.us-east-1.ooyala.com/hastur.sh |sudo bash -s || echo "Agent install/upgrade failed, skipping"

if ! [ -f /etc/uuid ]
  then uuidgen > /etc/uuid
fi

if ! [ -f /usr/lib64/libzmq.so.1.0.1 ]
  then cp libzmq.so.1.0.1 /usr/lib64/libzmq.so.1.0.1
  ln -s /usr/lib64/libzmq.so.1.0.1 /usr/lib64/libzmq.so.1
  ln -s /usr/lib64/libzmq.so.1.0.1 /usr/lib64/libzmq.so
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
