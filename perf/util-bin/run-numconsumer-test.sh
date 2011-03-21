#!/bin/bash

. `dirname $0`/remote-kafka-env.sh

REMOTE_KAFKA_HOST=$1 # todo: use this instead of localhost for -kafkaServer=localhost (@ problem)
REMOTE_SIM_HOST=$2
TEST_TIME=$3
REPORT_FILE=$4

kafka_startup
# You need to twidle this time value depending on test time below
ssh $REMOTE_SIM_HOST "$SIMULATOR_SCRIPT -kafkaServer=localhost -numTopic=10  -reportFile=$REPORT_FILE -time=7 -numConsumer=0 -numProducer=10 -xaxis=numConsumer"
sleep 20

for i in 1 `seq -s " " 10 10 50` ;
do
    ssh $REMOTE_SIM_HOST "$SIMULATOR_SCRIPT -kafkaServer=localhost -numTopic=10  -reportFile=$REPORT_FILE -time=$TEST_TIME -numConsumer=$i -numProducer=0 -xaxis=numConsumer"
    sleep 10
done

kafka_cleanup
