#!/bin/bash

. `dirname $0`/remote-kafka-env.sh

REMOTE_KAFKA_HOST=$1 # todo: use this instead of localhost for -kafkaServer=localhost (@ problem)
REMOTE_SIM_HOST=$2
TEST_TIME=$3
REPORT_FILE=$4

for i in 1 `seq -s " " 10 10 50` ;
do
    kafka_startup
    ssh $REMOTE_SIM_HOST "$SIMULATOR_SCRIPT -kafkaServer=localhost -numTopic=10  -reportFile=$REPORT_FILE -time=$TEST_TIME -numConsumer=20 -numProducer=40 -xaxis=fetchSize -msgSize=1000 -fetchSize=$((1024*$i))"
    kafka_cleanup
done
