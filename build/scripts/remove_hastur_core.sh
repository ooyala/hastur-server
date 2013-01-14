#!/bin/bash

echo "Remove old Hastur core, if present..."
/etc/init.d/hastur-core.init stop || echo "No old Hastur-core.  No problem."
update-rc.d hastur-core.init remove || echo "No old Hastur-core.  No problem."
rm -f /etc/init.d/hastur-core.init
# Don't remove /etc/hastur since that would take out the agent files too.

echo "Remove v2 Hastur core, if present..."
