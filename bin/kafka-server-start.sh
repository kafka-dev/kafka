#!/bin/bash

if [ $# -lt 1 ];
then
	echo "USAGE: $0 server.properties [consumer.properties]"
	exit 1
fi

$(dirname $0)/kafka-run-class.sh kafka.Kafka $@
