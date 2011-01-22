#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)/../..

# include kafka jars
for file in $base_dir/dist/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

for file in $base_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done


local_dir=$(dirname $0)

# include hadoop-consumer jars
for file in $local_dir/lib/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

CLASSPATH=dist:$CLASSPATH:${HADOOP_HOME}/conf

#if [ -z "$KAFKA_OPTS" ]; then
#  KAFKA_OPTS="-Xmx512M -server -Dcom.sun.management.jmxremote"
#fi

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA $KAFKA_OPTS -cp $CLASSPATH $@
