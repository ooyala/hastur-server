#!/bin/bash

export JAVA_OPTS="-XX:+UseParNewGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=100 -XX:GCTimeRatio=19"
export RESOURCE_OPTS="-Xmx1g"

pkill -9 -f core.jar

export JAVA_CMD="java -jar core.jar $RESOURCE_OPTS $JAVA_OPTS --router tcp://0.0.0.0:8126"
sudo -u role-hastur -g role-hastur $JAVA_CMD &
