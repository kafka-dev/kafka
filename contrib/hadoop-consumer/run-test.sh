#!/bin/bash

if [ $# -lt 1 ];
then
  echo "USAGE: $0 classname [opts]"
  exit 1
fi

base_dir=$(dirname $0)

CLASSPATH=$CLASSPATH:${HADOOP_HOME}/conf

for file in $base_dir/dist/kafka-etl-test-*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

$JAVA -cp $CLASSPATH $@
