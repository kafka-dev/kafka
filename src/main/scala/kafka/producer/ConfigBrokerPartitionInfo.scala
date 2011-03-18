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

import collection.mutable.HashMap
import collection.mutable.Map
import org.apache.log4j.Logger
import collection.SortedSet
import kafka.cluster.{Broker, Partition}
import kafka.common.InvalidConfigException

private[producer] class ConfigBrokerPartitionInfo(config: ProducerConfig) extends BrokerPartitionInfo {
  private val logger = Logger.getLogger(classOf[ConfigBrokerPartitionInfo])
  private val brokerPartitions: SortedSet[Partition] = getConfigTopicPartitionInfo
  private val allBrokers = getConfigBrokerInfo
  private val topicPartitions: Map[String, SortedSet[Partition]] = getTopicPartitions
  /**
   * Return a sequence of (brokerId, numPartitions)
   * @param topic this value is null 
   * @return a sequence of (brokerId, numPartitions)
   */
  def getBrokerPartitionInfo(topic: String): SortedSet[Partition] =
    topicPartitions.getOrElse(topic, brokerPartitions)

  /**
   * Generate the host and port information for the broker identified
   * by the given broker id
   * @param brokerId the broker for which the info is to be returned
   * @return host and port of brokerId
   */
  def getBrokerInfo(brokerId: Int): Option[Broker] = {
    allBrokers.get(brokerId)
  }

  /**
   * Generate a mapping from broker id to the host and port for all brokers
   * @return mapping from id to host and port of all brokers
   */
  def getAllBrokerInfo: Map[Int, Broker] = allBrokers

  def close {}

  private def getTopicPartitions: Map[String, SortedSet[Partition]] = {
    val numPartitionsPerTopic = new HashMap[String, SortedSet[Partition]]()

    if(config.topicPartitions != null) {
      val numEntries = config.topicPartitions.split(",")
      for(entry <- numEntries) {
        val topicNumPartitions = entry.split(":")
        if(topicNumPartitions.length != 2)
          throw new InvalidConfigException("Each entry in topic.num.partitions should be of the format => " +
                                           "topic:numPartitions")
        var partitions = SortedSet.empty[Partition]
        brokerPartitions.foreach { bp =>
          for(i <- 0 until topicNumPartitions(1).toInt) {
            val bidPid = new Partition(bp.brokerId, i)
            partitions += bidPid
          }
        }
        numPartitionsPerTopic += topicNumPartitions(0) -> partitions
      }
    }
    numPartitionsPerTopic
  }

  /**
   * Generate a sequence of (brokerId, numPartitions) for all brokers
   * specified in the producer configuration
   * @return sequence of (brokerId, numPartitions)
   */
  private def getConfigTopicPartitionInfo(): SortedSet[Partition] = {
    val brokerInfoList = config.brokerPartitionInfo.split(",")
    if(brokerInfoList.size == 0) throw new InvalidConfigException("broker.partition.info has invalid value")
    // check if each individual broker info is valid => (brokerId: brokerHost: brokerPort: numPartitions)
    brokerInfoList.foreach { bInfo =>
      val brokerInfo = bInfo.split(":")
      if(brokerInfo.size != 4) throw new InvalidConfigException("broker.partition.info has invalid value")
    }
    val brokerPartitions = brokerInfoList.map(bInfo => (bInfo.split(":").head.toInt, bInfo.split(":").last.toInt))
    var brokerParts = SortedSet.empty[Partition]
    brokerPartitions.foreach { bp =>
      for(i <- 0 until bp._2) {
        val bidPid = new Partition(bp._1, i)
        brokerParts = brokerParts + bidPid
      }
    }
    brokerParts
  }

  /**
   * Generate the host and port information for for all brokers
   * specified in the producer configuration
   * @return mapping from brokerId to (host, port) for all brokers
   */
  private def getConfigBrokerInfo(): Map[Int, Broker] = {
    val brokerInfo = new HashMap[Int, Broker]()
    val brokerInfoList = config.brokerPartitionInfo.split(",")
    brokerInfoList.foreach{ bInfo =>
      val brokerIdHostPort = bInfo.split(":").dropRight(1)
      brokerInfo += (brokerIdHostPort(0).toInt -> new Broker(brokerIdHostPort(0).toInt, brokerIdHostPort(1),
        brokerIdHostPort(1), brokerIdHostPort(2).toInt))
    }
    brokerInfo
  }

}