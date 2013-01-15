#!/bin/bash

echo "Remove old Hastur agent, if present..."
/etc/init.d/hastur-agent.init stop || echo "No old Hastur-agent.  No problem."
update-rc.d hastur-agent.init remove || echo "No old Hastur-agent.  No problem."
rm -f /etc/init.d/hastur-agent.init
rm -rf /opt/hastur || echo "No /etc/hastur.  No problem."  # This will take out any server files, too...
