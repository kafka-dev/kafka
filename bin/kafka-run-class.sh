#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$KAFKA_OPTS" ]; then
  KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote -Dlog4j.configuration=file:$base_dir/dist/log4j.properties "
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS -cp $CLASSPATH $@
