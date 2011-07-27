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

package kafka.server

import org.apache.log4j.Logger
import kafka.utils.{SystemTime, Utils}
import kafka.message.ByteBufferMessageSet
import kafka.api.RequestKeys
import kafka.consumer._

class KafkaServerStartable(val serverConfig: KafkaConfig, val consumerConfig: ConsumerConfig) {
  private var server : KafkaServer = null
  private var embeddedConsumer : EmbeddedConsumer = null

  init

  def this(serverConfig: KafkaConfig) = this(serverConfig, null)

  private def init() {
    server = new KafkaServer(serverConfig)
    if (consumerConfig != null)
      embeddedConsumer = new EmbeddedConsumer(consumerConfig, server)
  }

  def startup() {
    server.startup
    if (embeddedConsumer != null)
      embeddedConsumer.startup
  }

  def shutdown() {
    if (embeddedConsumer != null)
      embeddedConsumer.shutdown
    server.shutdown
  }

  def awaitShutdown() {
    server.awaitShutdown
  }
}

class EmbeddedConsumer(private val consumerConfig: ConsumerConfig,
                       private val kafkaServer: KafkaServer) extends TopicEventHandler[String] {
  private val logger = Logger.getLogger(getClass)

  def handleTopicEvent(addedTopics: Seq[String],
                       deletedTopics: Seq[String], allTopics: Seq[String]) {
    logger.info("topic event: new topics")
    for (topic <- addedTopics) {
      logger.info("\t%s".format(topic))
    }
    for (topic <- deletedTopics) {
      logger.info("\t%s".format(topic))
    }

    consumerConnector.shutdown()

    consumerConnector = Consumer.create(consumerConfig)

    startNewConsumerThreads()
  }

  private var consumerConnector: ConsumerConnector = null
  private val topicEventWatcher =
    new ZookeeperTopicEventWatcher(consumerConfig, Some(this))

  private def makeTopicMap() = {
    if (consumerConfig.mirrorTopicsWhitelistMap.nonEmpty) {
      consumerConfig.mirrorTopicsWhitelistMap
    }
    else {
      val allTopics = topicEventWatcher.topics
      val blackListTopics = consumerConfig.mirrorTopicsBlackList.split(",").toList.map(_.trim)
      val whiteListTopics = allTopics filterNot (blackListTopics contains)
      if (whiteListTopics.nonEmpty)
        Utils.getConsumerTopicMap(whiteListTopics.mkString("", ":1,", ":1"))
      else
        Utils.getConsumerTopicMap("")
    }
  }

  private def startNewConsumerThreads() {
    val topicMap = makeTopicMap()
    if (topicMap.size > 0) {
      if (consumerConnector != null) {
        consumerConnector.shutdown()
      }
      consumerConnector = Consumer.create(consumerConfig)
      val topicMessageStreams =  consumerConnector.createMessageStreams(makeTopicMap())
      var threadList = List[Thread]()
      for ((topic, streamList) <- topicMessageStreams)
        for (i <- 0 until streamList.length)
          threadList ::= Utils.newThread("kafka-embedded-consumer-" + topic + "-" + i, new Runnable() {
            def run() {
              logger.info("starting consumer thread " + i + " for topic " + topic)
              val logManager = kafkaServer.getLogManager()
              val stats = kafkaServer.getStats()
              try {
                for (message <- streamList(i)) {
                  val partition = logManager.chooseRandomPartition(topic)
                  val start = SystemTime.nanoseconds
                  logManager.getOrCreateLog(topic, partition).append(new ByteBufferMessageSet(message))
                  stats.recordRequest(RequestKeys.Produce, SystemTime.nanoseconds - start)
                }
              }
              catch {
                case e =>
                  logger.fatal(e + Utils.stackTrace(e))
                  logger.fatal(topic + " stream " + i + " unexpectedly exited")
              }
            }
          }, false)

      for (thread <- threadList)
        thread.start()
    }
  }

  def startup() {
    startNewConsumerThreads()
  }

  def shutdown() {
    consumerConnector.shutdown
  }
}
