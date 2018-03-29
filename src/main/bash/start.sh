#!/usr/bin/env bash

polaris=$(dirname $0)/..

conf=${polaris}/conf

JAR_FILE="${polaris}/lib/*.jar"

if [ -z ${JVM_OPTS} ]; then
  JVM_OPTS=" -Xms256m -Xmx1G"
fi

# Set the spring configuration file
if [ -f ${conf}/application.properties ]; then
  APP_OPTS="$APP_OPTS --spring.config.location=file:$conf/application.properties"
fi

# Set the log4j configuration file
if [ -f ${conf}/log4j.properties ]; then
    LOG4J_OPTS="-Dlog4j.configuration=file:${conf}/log4j.properties"
fi

java ${JVM_OPTS} ${LOG4J_OPTS} -jar ${JAR_FILE} ${APP_OPTS} $@ &

echo $! > ${polaris}/currentpid
