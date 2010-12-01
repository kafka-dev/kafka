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

package kafka

import api.RequestKeys
import consumer.{ConsumerConfig, ConsumerConnector, Consumer}
import kafka.utils._
import kafka.server._
import log.LogManager
import message.ByteBufferMessageSet
import network.SocketServerStats
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.apache.log4j.Logger
import scala.collection._

object Kafka {
  private val logger = Logger.getLogger(Kafka.getClass)

  def main(args: Array[String]): Unit = {
    if(args.length != 1 && args.length != 2) {
      println("USAGE: java [options] " + classOf[KafkaServer].getSimpleName() + " server.properties [consumer.properties")
      System.exit(1)
    }
  
    val props = Utils.loadProps(args(0))
    val server = new KafkaServer(new KafkaConfig(props))
    var embeddedConsumer: EmbeddedConsumer = null
    if (args.length == 2) {
      val consumerConfig = new ConsumerConfig(Utils.loadProps(args(1)))
      embeddedConsumer = new EmbeddedConsumer(consumerConfig,
                                              getConsumerTopicMap(consumerConfig.embeddedConsumerTopics),
                                              server)
    }

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        if (embeddedConsumer != null)
          embeddedConsumer.stop
        server.shutdown
      }
    });

    try {
      server.startup
      if (embeddedConsumer != null)
        embeddedConsumer.start
      server.awaitShutdown
    }
    catch {
      case e => logger.fatal(e)
    }
    System.exit(0)
  }

  private def getConsumerTopicMap(topicsString: String): Map[String, Int] = {
    val topicsMap = new mutable.HashMap[String, Int]
    val topics = topicsString.split(",")
    for (topic <- topics) {
      val topicAndThreads = topic.split(":")
      topicsMap.put(topicAndThreads(0), topicAndThreads(1).toInt)      
    }
    topicsMap
  }

}

class EmbeddedConsumer(private val consumerConfig: ConsumerConfig,
                       private val consumerTopicMap: Map[String, Int],
                       private val kafkaServer: KafkaServer) {
  private val logger = Logger.getLogger(getClass())
  private val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
  private val topicMessageStreams = consumerConnector.createMessageStreams(consumerTopicMap)

  def start() = {
    var threadList = List[Thread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= Utils.newThread("kafka-embedded-consumer-" + topic + "-" + i, new Runnable() {
          def run() {
            logger.info("starting consumer thread " + i + " for topic " + topic)
            val logManager = kafkaServer.getLogManager
            val stats = kafkaServer.getStats
            for (message <- streamList(i)) {
              val partition = logManager.chooseRandomPartition(topic)
              val start = SystemTime.nanoseconds
              logManager.getOrCreateLog(topic, partition).append(new ByteBufferMessageSet(message))
              stats.recordRequest(RequestKeys.Produce, SystemTime.nanoseconds - start)
            }
          }
        }, false)

    for (thread <- threadList)
      thread.start
  }

  def stop() = {
    consumerConnector.shutdown
  }
}