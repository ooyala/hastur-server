#!/bin/bash

ln -nfs /opt/hastur/server/bin/hastur-bluepill.init /etc/init.d/hastur-agent.init

update-rc.d hastur-agent.init defaults

/etc/init.d/hastur-agent.init restart

