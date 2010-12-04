#!/bin/bash
for i in 1 10 20 30 40 50;
do
 ./run-simulator.sh -kafkaServer=$1 -numTopic=$i  -reportFile=$2 -time=30 -numConsumer=20 -numProducer=40
done
