#!/bin/bash

# gem has been instructed not to install a shim for hastur-bluepill.init
# so it's buried in /opt/hastur. Find the first occurrence and symlink that.
initscript=$(find /opt/hastur -type f -name hastur-bluepill.init | head -n 1)

if [ -n "$initscript" ] ; then
  ln -nfs $initscript /etc/init.d/hastur-core.init
  update-rc.d hastur-core.init defaults
  /etc/init.d/hastur-core.init restart

  ln -nfs $initscript /etc/init.d/hastur-agent.init
  update-rc.d hastur-agent.init defaults
  /etc/init.d/hastur-agent.init restart
else
  echo "Could not find hastur-bluepill.init. Please notify team-tna@ooyala.com."
  exit 1
fi

