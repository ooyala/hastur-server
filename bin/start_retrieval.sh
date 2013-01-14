#!/bin/bash

export JAVA_OPTS="-XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=100 -XX:GCTimeRatio=19"
export RESOURCE_OPTS="-Xmx5g"

pkill -9 -f retrieval_v2.war

export JAVA_CMD="java -jar retrieval_v2.war $RESOURCE_OPTS $JAVA_OPTS"
sudo -u role-hastur -g role-hastur $JAVA_CMD &
