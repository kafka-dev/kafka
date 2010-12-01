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

package kafka.integration

import junit.framework.TestCase
import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import kafka.server.{KafkaServer, KafkaConfig}
import java.nio.channels.ClosedByInterruptException
import org.apache.log4j.Logger
import java.util.concurrent.atomic.AtomicInteger
import kafka.{TestZKUtils, TestUtils}
import kafka.utils.ZKGroupTopicDirs
import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig, ConsumerConnector, Consumer}

class AutoOffsetResetTest extends TestCase with ZooKeeperTestHarness {

  val zkConnect = TestZKUtils.zookeeperConnect
  val topic = "test_topic"
  val group = "default_group"
  val testConsumer = "consumer"
  val brokerPort = 9892
  val kafkaConfig = new KafkaConfig(TestUtils.createBrokerConfig(0, brokerPort))
  var kafkaServer : KafkaServer = null
  val numMessages = 10
  val largeOffset = 10000
  val smallOffset = -1
  
  private val logger = Logger.getLogger(getClass())

  override def setUp() {
    super.setUp()
    kafkaServer = TestUtils.createServer(kafkaConfig)
  }

  override def tearDown() {
    kafkaServer.shutdown
    super.tearDown
  }
  
  def testEarliestOffsetResetForward() = {
    val producer = TestUtils.createProducer("localhost", brokerPort)

    for(i <- 0 until numMessages) {
      producer.send(topic, TestUtils.singleMessageSet("test".getBytes()))
    }

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", "smallest")
    consumerProps.put("consumer.timeout.ms", "2000")
    val consumerConfig = new ConsumerConfig(consumerProps)
    
    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0-0", largeOffset)
    logger.info("Updated consumer offset to " + largeOffset)


    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStreams = consumerConnector.createMessageStreams(Map(topic -> 1))

    var threadList = List[Thread]()
    val nMessages : AtomicInteger = new AtomicInteger(0)
    for ((topic, streamList) <- messageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new Thread("kafka-zk-consumer-" + i) {
          override def run() {

            try {
              for (message <- streamList(i)) {
                nMessages.incrementAndGet
              }
            }
            catch {
              case te: ConsumerTimeoutException => logger.info("Consumer thread timing out..")
              case _: InterruptedException => 
              case _: ClosedByInterruptException =>
              case e => throw e
            }
          }

        }


    for (thread <- threadList)
      thread.start

    threadList(0).join(1100)

    logger.info("Asserting...")
    assertEquals(numMessages, nMessages.get)
    consumerConnector.shutdown
  }

  def testEarliestOffsetResetBackward() = {
    val producer = TestUtils.createProducer("localhost", brokerPort)

    for(i <- 0 until numMessages) {
      producer.send(topic, TestUtils.singleMessageSet("test".getBytes()))
    }

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", "smallest")
    consumerProps.put("consumer.timeout.ms", "2000")
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0-0", smallOffset)
    logger.info("Updated consumer offset to " + smallOffset)


    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStreams = consumerConnector.createMessageStreams(Map(topic -> 1))

    var threadList = List[Thread]()
    val nMessages : AtomicInteger = new AtomicInteger(0)
    for ((topic, streamList) <- messageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new Thread("kafka-zk-consumer-" + i) {
          override def run() {

            try {
              for (message <- streamList(i)) {
                nMessages.incrementAndGet
              }
            }
            catch {
              case _: InterruptedException => 
              case _: ClosedByInterruptException =>
              case e => throw e
            }
          }

        }


    for (thread <- threadList)
      thread.start

    threadList(0).join(1100)

    logger.info("Asserting...")
    assertEquals(numMessages, nMessages.get)
    consumerConnector.shutdown
  }

  def testLatestOffsetResetForward() = {
    val producer = TestUtils.createProducer("localhost", brokerPort)

    for(i <- 0 until numMessages) {
      producer.send(topic, TestUtils.singleMessageSet("test".getBytes()))
    }

    // update offset in zookeeper for consumer to jump "forward" in time
    val dirs = new ZKGroupTopicDirs(group, topic)
    var consumerProps = TestUtils.createConsumerProperties(zkConnect, group, testConsumer)
    consumerProps.put("autooffset.reset", "largest")
    consumerProps.put("consumer.timeout.ms", "2000")
    val consumerConfig = new ConsumerConfig(consumerProps)

    TestUtils.updateConsumerOffset(consumerConfig, dirs.consumerOffsetDir + "/" + "0-0", largeOffset)
    logger.info("Updated consumer offset to " + largeOffset)


    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val messageStreams = consumerConnector.createMessageStreams(Map(topic -> 1))

    var threadList = List[Thread]()
    val nMessages : AtomicInteger = new AtomicInteger(0)
    for ((topic, streamList) <- messageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new Thread("kafka-zk-consumer-" + i) {
          override def run() {

            try {
              for (message <- streamList(i)) {
                nMessages.incrementAndGet
              }
            }
            catch {
              case _: InterruptedException => 
              case _: ClosedByInterruptException =>
              case e => throw e
            }
          }

        }


    for (thread <- threadList)
      thread.start

    threadList(0).join(1100)

    logger.info("Asserting...")

    assertEquals(0, nMessages.get)
    consumerConnector.shutdown
  }

  
}
