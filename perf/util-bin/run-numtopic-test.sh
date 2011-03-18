#!/bin/bash

for i in 1 10 20 30 40 50;
do
    ssh $1 "cd $KAFKA_HOME; ./kafka-server.sh server.properties 2>&1 >kafka.out" &
    sleep 60
    ./run-simulator.sh -kafkaServer=$1 -numTopic=$i  -reportFile=$2 -time=15 -numConsumer=20 -numProducer=40 -xaxis=numTopic
    ssh $1 "cd $KAFKA_HOME; ./stop-server.sh" &
    sleep 20
    ssh $1 "rm -rf /tmp/kafka-logs" &
    sleep 40
done
