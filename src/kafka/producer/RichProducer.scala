/*
 * Copyright 2010 LinkedIn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.producer

import org.I0Itec.zkclient.ZkClient
import kafka.utils.{StringSerializer, ZkUtils, Utils}
import org.apache.log4j.Logger
import kafka.message.{ByteBufferMessageSet, Message}

class RichProducer[T](config: RichProducerConfig,
                      partitioner: Partitioner) {
  private val logger = Logger.getLogger(classOf[RichProducer[T]])

  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                                      StringSerializer)

  def this(config: RichProducerConfig) {
    this(config, Utils.getObject(config.partitionerClass))
  }

  def send(topic: String, data: Message) {
    // find the number of broker partitions registered for this topic
    val brokerTopicPath = ZkUtils.brokerTopicsPath + "/" + topic
    val brokerList = ZkUtils.getChildren(zkClient, brokerTopicPath)
    val numPartitions = brokerList.map(bid => ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid).toInt)
    val totalNumPartitions = numPartitions.reduceLeft(_ + _)
    logger.info("Total number of partitions for this topic = " + totalNumPartitions)
    
    val brokerIds = brokerList.map(bid => bid.toInt).sortWith((id1, id2) => id1 < id2)
    logger.info("Sorted list of broker ids = " + brokerIds.toString)
    
    val partitionId = partitioner.partition(data, totalNumPartitions)
    logger.info("Selected partition id = " + partitionId)
    if(partitionId < 0 || partitionId >= totalNumPartitions)
      throw new InvalidPartitionException("Invalid partition id : " + partitionId +
              "\n Valid values are in the range inclusive [0, " + (totalNumPartitions-1) + "]")

    var brokerParts: List[(Int, Int)] = Nil
    brokerIds.zip(numPartitions).foreach { bp =>
      for(i <- 0 until bp._2) {
        brokerParts ::= (bp._1, i)
      }      
    }
    brokerParts = brokerParts.reverse
    logger.info("Broker parts = " + brokerParts.toString)

    val brokerPort = brokerParts(partitionId)
    // find the host and port of the selected broker id
    val brokerInfo = ZkUtils.readData(zkClient, ZkUtils.brokerIdsPath + "/" + brokerPort._1)
    val brokerHostPort = brokerInfo.split(":")
    logger.info("Sending message to broker " + brokerHostPort(1) + ":" + brokerHostPort(2) + " on partition " +
      brokerPort._2)

    val producer = new SimpleProducer(brokerHostPort(1), brokerHostPort(2).toInt, config.bufferSize,
      config.connectTimeoutMs, config.reconnectInterval)
    producer.send(topic, brokerPort._2, new ByteBufferMessageSet(data))
  }
}