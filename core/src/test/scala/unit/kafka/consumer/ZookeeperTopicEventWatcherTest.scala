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

package unit.kafka.consumer

import org.scalatest.junit.JUnit3Suite
import junit.framework.Assert._
import kafka.zk.ZooKeeperTestHarness
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{StringSerializer, ZkUtils, TestUtils, TestZKUtils}
import org.apache.log4j.{Level, Logger}
import java.util.Properties
import kafka.consumer.{TopicEventHandler, ConsumerConfig, ZookeeperTopicEventWatcher}

class ZookeeperTopicEventWatcherTest extends JUnit3Suite
  with ZooKeeperTestHarness with TopicEventHandler[String] {

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  private def pathToTopic(i: Int) = {
    "%s/%s-%d".format(ZkUtils.brokerTopicsPath, "topic", i)
  }

  def handleTopicEvent(addedTopics: Seq[String], deletedTopics: Seq[String],
                       allTopics: Seq[String]) {
//    logger.info("event handler:")
//    for (val topic <- addedTopics) {
//      logger.info("\tadded topic %s".format(topic))
//    }
//    for (val topic <- deletedTopics) {
//      logger.info("\tdeleted topic %s".format(topic))
//    }
  }

  val zkConnect = TestZKUtils.zookeeperConnect

  private val consumerConfig = new ConsumerConfig(
    TestUtils.createConsumerProperties(
      zkConnect, "groupid_dontcare", "consumerid_dontcare"))

  def testAll() {
    val numTopicsToAdd = 100
    val numTopicsToDelete = 10 % numTopicsToAdd

    val zkClient = new ZkClient(zkConnect, consumerConfig.zkSessionTimeoutMs,
      consumerConfig.zkConnectionTimeoutMs, StringSerializer)

    // start with no topics
    val topicEventWatcher = new ZookeeperTopicEventWatcher(
      consumerConfig, Some(this))
    assertEquals(topicEventWatcher.topics.size, 0)

    // add a few topics
    for (topicNum <- 1 to numTopicsToAdd)
      ZkUtils.createEphemeralPathExpectConflict(
        zkClient, pathToTopic(topicNum), "data_dontcare")

    Thread.sleep(2000)

    assertEquals(topicEventWatcher.topics.size, numTopicsToAdd)

    // delete some topics
    for (topicNum <- 1 to numTopicsToDelete)
      ZkUtils.deletePath(zkClient, pathToTopic(topicNum))

    Thread.sleep(2000)

    assertEquals(topicEventWatcher.topics.size,
      numTopicsToAdd - numTopicsToDelete)

    // delete all topics
    for (topicNum <- numTopicsToDelete to numTopicsToAdd)
      ZkUtils.deletePath(zkClient, pathToTopic(topicNum))

    Thread.sleep(2000)

    assertEquals(topicEventWatcher.topics.size, 0)
  }

}
