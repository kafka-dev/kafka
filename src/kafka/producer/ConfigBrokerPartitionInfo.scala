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
import kafka.common.InvalidConfigException
import org.apache.log4j.Logger

class ConfigBrokerPartitionInfo(config: ProducerConfig) extends BrokerPartitionInfo {
  private val logger = Logger.getLogger(classOf[ConfigBrokerPartitionInfo])
  private val brokerPartitions: Seq[(Int, Int)] = getConfigTopicPartitionInfo
  private val allBrokers = getConfigBrokerInfo

  /**
   * Return a sequence of (brokerId, numPartitions)
   * @param topic this value is null 
   * @return a sequence of (brokerId, numPartitions)
   */
  def getBrokerPartitionInfo(topic: String = null): Seq[(Int, Int)] = brokerPartitions

  /**
   * Generate the host and port information for the broker identified
   * by the given broker id
   * @param brokerId the broker for which the info is to be returned
   * @return host and port of brokerId
   */
  def getBrokerInfo(brokerId: Int): Option[(String, Int)] = {
    allBrokers.get(brokerId)
  }

  /**
   * Generate a mapping from broker id to the host and port for all brokers
   * @return mapping from id to host and port of all brokers
   */
  def getAllBrokerInfo: Map[Int, (String, Int)] = allBrokers

  /**
   * Generate a sequence of (brokerId, numPartitions) for all brokers
   * specified in the producer configuration
   * @return sequence of (brokerId, numPartitions)
   */
  private def getConfigTopicPartitionInfo(): Seq[(Int, Int)] = {
    val brokerInfoList = config.brokerPartitionInfo.split(",")
    if(brokerInfoList.size == 0) throw new InvalidConfigException("broker.partition.info has invalid value")
    // check if each individual broker info is valid => (brokerId: brokerHost: brokerPort: numPartitions)
    brokerInfoList.foreach { bInfo =>
      val brokerInfo = bInfo.split(":")
      if(brokerInfo.size != 4) throw new InvalidConfigException("broker.partition.info has invalid value")
    }
    brokerInfoList.map(bInfo => (bInfo.split(":").first.toInt, bInfo.split(":").last.toInt))
  }

  /**
   * Generate the host and port information for for all brokers
   * specified in the producer configuration
   * @return mapping from brokerId to (host, port) for all brokers
   */
  private def getConfigBrokerInfo(): Map[Int, (String, Int)] = {
    val brokerInfo = new HashMap[Int, (String, Int)]()
    val brokerInfoList = config.brokerPartitionInfo.split(",")
    brokerInfoList.foreach{ bInfo =>
      val brokerIdHostPort = bInfo.split(":").dropRight(1)
      brokerInfo += (brokerIdHostPort(0).toInt -> (brokerIdHostPort(1), brokerIdHostPort(2).toInt))
    }
    brokerInfo
  }

}