#!/bin/bash

export GC_OPTS="-XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=100 -XX:GCTimeRatio=19"
export RESOURCE_OPTS="-Xmx5g"
export JETTY_OPTS="-Djetty.port=8077"

export JAVA_OPTS="$GC_OPTS $RESOURCE_OPTS $JETTY_OPTS"

# Configure Hastur's Cassandra settings
# export HASTUR_CASS_PORT=9161
# export HASTUR_CASS_CLUSTER=aCluster
# export HASTUR_CASS_USER=hastur
# export HASTUR_CASS_PASSWD=hasturPassword

pkill -9 -f retrieval_v2.war

exec java $JAVA_OPTS -jar retrieval_v2.war
