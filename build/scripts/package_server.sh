#!/bin/bash

# This builds a deployable file to copy to the Hastur core/retrieval servers.
# Run it from root of hastur-server project by calling bin/package_server.sh.

cp bin/install_server.sh build/server/

# TODO: move all these scripts under build/

# Build jars, copy into build/server/
rm -f jars/*.?ar
mkdir -p build/server
rake core_jar
cp core.jar build/server/
rake retrieval_war
cp retrieval_v2.war build/server/

# Hastur Core scripts
cp bin/hastur-core.conf build/server/
cp bin/start_core.sh build/server/

# Hastur Retrieval v2 scripts
cp bin/hastur-retrieval.conf build/server/
cp bin/start_retrieval.sh build/server/

# Manually-runnable removal scripts for old versions
cp bin/remove*.sh build/server/
