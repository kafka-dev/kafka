#!/bin/bash

num_messages=10000000
message_size=400

base_dir=$(dirname $0)/..

rm -rf /tmp/zookeeper_source
rm -rf /tmp/zookeeper_target
rm -rf /tmp/kafka-source-logs
rm -rf /tmp/kafka-target-logs

echo "start the servers ..."
$base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_source.properties 2>&1 > $base_dir/zookeeper_source.log &
$base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_target.properties 2>&1 > $base_dir/zookeeper_target.log &
$base_dir/../../bin/kafka-server-start.sh $base_dir/config/server_source.properties 2>&1 > $base_dir/kafka_source.log &
$base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target.properties $base_dir/config/consumer.properties 2>&1 > $base_dir/kafka_target.log &

sleep 4
echo "start producing messages ..."
$base_dir/../../bin/kafka-run-class.sh kafka.tools.ProducerPerformance --server kafka://localhost:9092 --topic test01 --messages $num_messages --message-size $message_size --batch-size 200 --vary-message-size --threads 1

echo "wait for consumer to finish consuming ..."
cur_offset="-1"
quit=0
while [ $quit -eq 0 ]
do
  sleep 2
  target_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
  if [ $target_size == $cur_offset ]
  then
    quit=1
  fi
  cur_offset=$target_size
done

sleep 2
source_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9092 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
target_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`

if [ $source_size != $target_size ]
then
   echo "source size: $source_size target size: $target_size test failed!!! look at it!!!"
else
   echo "test passed"
fi

ps ax | grep -i 'kafka.kafka' | grep -v grep | awk '{print $1}' | xargs kill -15 > /dev/null
sleep 2
ps ax | grep -i 'QuorumPeerMain' | grep -v grep | awk '{print $1}' | xargs kill -15 > /dev/null

