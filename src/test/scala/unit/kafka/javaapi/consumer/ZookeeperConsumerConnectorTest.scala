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

package kafka.javaapi.consumer

import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import kafka.javaapi.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.message.{Message}
import org.apache.log4j.Logger
import scala.collection._
import kafka.utils.Utils
import kafka.{TestZKUtils, TestUtils}
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.consumer.{ConsumerConfig, KafkaMessageStream, ConsumerTimeoutException}
import scala.collection.JavaConversions._
import kafka.javaapi.message.ByteBufferMessageSet

class ZookeeperConsumerConnectorTest extends JUnitSuite with KafkaServerTestHarness with ZooKeeperTestHarness {
  private val logger = Logger.getLogger(getClass())

  val zookeeperConnect = TestZKUtils.zookeeperConnect
  val zkConnect = zookeeperConnect
  val numNodes = 2
  val numParts = 2
  val topic = "topic1"
  val configs =
    for(props <- TestUtils.createBrokerConfigs(numNodes))
      yield new KafkaConfig(props) {
        override val enableZookeeper = true
        override val numPartitions = numParts
        override val zkConnect = zookeeperConnect
      }
  val group = "group1"
  val consumer0 = "consumer0"
  val consumer1 = "consumer1"
  val consumer2 = "consumer2"
  val consumer3 = "consumer3"
  val nMessages = 2

  @Test
  def testBasic() {
    var actualMessages: List[Message] = Nil

    // test consumer timeout logic
    val consumerConfig0 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer0)) {
      override val consumerTimeoutMs = 200
    }
    val zkConsumerConnector0 = new ZookeeperConsumerConnector(consumerConfig0, true)
    val topicMessageStreams0 = zkConsumerConnector0.createMessageStreams(toJavaMap(Predef.Map(topic -> numNodes*numParts/2)))
    try {
      getMessages(nMessages*2, topicMessageStreams0)
      fail("should get an exception")
    }
    catch {
      case e: ConsumerTimeoutException => // this is ok
      case e => throw e
    }
    zkConsumerConnector0.shutdown

    // send some messages to each broker
    val sentMessages1 = sendMessages(nMessages, "batch1")
    // create a consumer
    val consumerConfig1 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer1))
    val zkConsumerConnector1 = new ZookeeperConsumerConnector(consumerConfig1, true)
    val topicMessageStreams1 = zkConsumerConnector1.createMessageStreams(toJavaMap(Predef.Map(topic -> numNodes*numParts/2)))
    val receivedMessages1 = getMessages(nMessages*2, topicMessageStreams1)
    assertEquals(sentMessages1, receivedMessages1)
    // commit consumed offsets
    zkConsumerConnector1.commitOffsets

    // create a consumer
    val consumerConfig2 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer2))
    val zkConsumerConnector2 = new ZookeeperConsumerConnector(consumerConfig2, true)
    val topicMessageStreams2 = zkConsumerConnector2.createMessageStreams(toJavaMap(Predef.Map(topic -> numNodes*numParts/2)))
    // send some messages to each broker
    val sentMessages2 = sendMessages(nMessages, "batch2")
    Thread.sleep(200)
    val receivedMessages2_1 = getMessages(nMessages, topicMessageStreams1)
    val receivedMessages2_2 = getMessages(nMessages, topicMessageStreams2)
    val receivedMessages2 = (receivedMessages2_1 ::: receivedMessages2_2).sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sentMessages2, receivedMessages2)

    // create a consumer with empty map
    val consumerConfig3 = new ConsumerConfig(
      TestUtils.createConsumerProperties(zkConnect, group, consumer3))
    val zkConsumerConnector3 = new ZookeeperConsumerConnector(consumerConfig3, true)
    val topicMessageStreams3 = zkConsumerConnector3.createMessageStreams(toJavaMap(new mutable.HashMap[String, Int]()))
    // send some messages to each broker
    Thread.sleep(200)
    val sentMessages3 = sendMessages(nMessages, "batch3")
    Thread.sleep(200)
    val receivedMessages3_1 = getMessages(nMessages, topicMessageStreams1)
    val receivedMessages3_2 = getMessages(nMessages, topicMessageStreams2)
    val receivedMessages3 = (receivedMessages3_1 ::: receivedMessages3_2).sortWith((s,t) => s.checksum < t.checksum)
    assertEquals(sentMessages3, receivedMessages3)

    zkConsumerConnector1.shutdown
    zkConsumerConnector2.shutdown
    zkConsumerConnector3.shutdown
    logger.info("all consumer connectors stopped")
  }

  def sendMessages(messagesPerNode: Int, header: String): List[Message]= {
    var messages: List[Message] = Nil
    for(conf <- configs) {
      val producer = kafka.javaapi.Implicits.toJavaSyncProducer(TestUtils.createProducer("localhost", conf.port))
      for (partition <- 0 until numParts) {
        val ms = 0.until(messagesPerNode).map(x =>
          new Message((header + conf.brokerId + "-" + partition + "-" + x).getBytes)).toArray
        val mSet = new ByteBufferMessageSet(getMessageList(ms: _*))
        for (message <- ms)
          messages ::= message
        producer.send(topic, partition, mSet)
      }
      producer.close()
    }
    messages.sortWith((s,t) => s.checksum < t.checksum)
  }

  def getMessages(nMessagesPerThread: Int, jTopicMessageStreams: java.util.Map[String, java.util.List[KafkaMessageStream]])
  : List[Message]= {
    var messages: List[Message] = Nil
    val topicMessageStreams = asMap(jTopicMessageStreams)
    for ((topic, messageStreams) <- topicMessageStreams) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator
        for (i <- 0 until nMessagesPerThread) {
          assertTrue(iterator.hasNext)
          val message = iterator.next
          messages ::= message
          logger.debug("received message: " + Utils.toString(message.payload, "UTF-8"))
        }
      }
    }
    messages.sortWith((s,t) => s.checksum < t.checksum)
  }

  private def getMessageList(messages: Message*): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messages.foreach(m => messageList.add(m))
    messageList
  }

  private def toJavaMap(scalaMap: Map[String, Int]): java.util.Map[String, java.lang.Integer] = {
    val javaMap = new java.util.HashMap[String, java.lang.Integer]()
    scalaMap.foreach(m => javaMap.put(m._1, m._2.asInstanceOf[java.lang.Integer]))
    javaMap
  }
}