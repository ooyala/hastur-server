#!/bin/bash

export JAVA_OPTS="-XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=100 -XX:GCTimeRatio=19"
export RESOURCE_OPTS="-Xmx1g"

# Configure Cassandra this way
# export HASTUR_CASS_PORT=9161
# export HASTUR_CASS_CLUSTER=aCluster
# export HASTUR_CASS_USER=hastur
# export HASTUR_CASS_PASSWD=hasturPassword

pkill -9 -f core.jar

export JAVA_CMD="java -jar core.jar $RESOURCE_OPTS $JAVA_OPTS --router tcp://0.0.0.0:8126"
sudo su role-hastur -s /bin/bash -c "$JAVA_CMD" &
