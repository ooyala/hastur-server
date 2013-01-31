#!/bin/bash

# gem has been instructed not to install a shim for hastur-bluepill.init
# so it's buried in /opt/hastur. Find the first occurrence and symlink that.
initscript=$(find /opt/hastur -type f -name hastur-bluepill.init | head -n 1)

# create the user if necessary
(grep -q '^role-hastur:' /etc/group)  || groupadd -g 60442 role-hastur
(grep -q '^role-hastur:' /etc/passwd) || useradd -r -o -u 60442 -c "Hastur Metrics Agent" -g role-hastur -s /bin/false role-hastur

if [ -n "$initscript" ] ; then
  ln -nfs $initscript /etc/init.d/hastur-agent.init
  update-rc.d hastur-agent.init defaults
  /etc/init.d/hastur-agent.init stop
  /etc/init.d/hastur-agent.init quit
  /etc/init.d/hastur-agent.init start
else
  echo "Could not find hastur-bluepill.init. Please notify appsplat-oncall@ooyala.com."
  exit 1
fi
