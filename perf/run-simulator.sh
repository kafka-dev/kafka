#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/..

#CLASSPATH=$CLASSPATH:bin
for file in $base_dir/lib/*.jar;
do
  if [ ${file##*/} != "sbt-launch.jar" ]; then
      CLASSPATH=$CLASSPATH:$file
  fi
done
for file in $base_dir/lib_managed/scala_2.8.0/compile/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done
for file in $base_dir/target/scala_2.8.0/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done
for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done


echo $CLASSPATH

if [ -z "$KAFKA_PERF_OPTS" ]; then
  KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=3333 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS -cp $CLASSPATH kafka.perf.KafkaPerfSimulator $@
