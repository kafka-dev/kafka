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

package kafka.consumer

import scala.collection._
import kafka.utils.Utils
import org.apache.log4j.Logger

/**
 *  Main interface for consumer
 */
trait ConsumerConnector {
  /**
   *  create a list of MessageStreams for each topic.
   */
  def createMessageStreams(topicCountMap: Map[String,Int]) : Map[String,List[KafkaMessageStream]]

  /**
   *  create a list of MessageStreams for each topic.
   *  java binding.
   */
  def createMessageStreams(topicCountMap: java.util.Map[String,java.lang.Integer]):
    java.util.Map[String,java.util.List[KafkaMessageStream]]

  /**
   *  Commit the offsets of all broker partitions connected by this connector.
   */
  def commitOffsets
  
  /**
   *  shut down the connector
   */
  def shutdown()
}

object Consumer {
  private val logger = Logger.getLogger(getClass())  
  private val consumerStatsMBeanName = "kafka:type=kafka.ConsumerStats"

  // constructor of ConsumerConnector
  def create(config: ConsumerConfig): ConsumerConnector = {
    val consumerConnect = new ZookeeperConsumerConnector(config)
    Utils.swallow(logger.warn, Utils.registerMBean(consumerConnect, consumerStatsMBeanName))
    consumerConnect
  }
}