#!/bin/bash

ssh $1 "cd $KAFKA_HOME; ./kafka-server.sh server.properties 2>&1 >kafka.out" &
./run-simulator.sh -kafkaServer=$1 -numTopic=10  -reportFile=$2 -time=2 -numConsumer=0 -numProducer=40 -numParts=1 -xaxis=numConsumer
sleep 2*1000

for i in 10 20 30 40 50;
do
    ./run-simulator.sh -kafkaServer=$1 -numTopic=10  -reportFile=$2 -time=1 -numConsumer=$i -numProducer=0 -xaxis=numConsumer
    sleep 10
done
ssh $1 "cd $KAFKA_HOME; ./stop-server.sh" &
sleep 20
ssh $1 "rm -rf /tmp/kafka-logs" &
sleep 40
