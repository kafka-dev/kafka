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

import org.apache.log4j.Logger
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import kafka.utils.{ZkUtils, StringSerializer}
import scala.collection.JavaConversions._
import collection.immutable.List

class ZookeeperTopicEventWatcher(val config:ConsumerConfig,
    val eventHandler: Option[TopicEventHandler[String]]) {

  private val logger = Logger.getLogger(getClass)
  private val zkClient: ZkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs,
      config.zkConnectionTimeoutMs, StringSerializer)
  var topics: List[String] = List()

  watchTopicEvents()

  def this(config:ConsumerConfig) {
    this(config, null)
  }

  private def watchTopicEvents() {
    val topicEventListener = new ZkTopicEventListener
    ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.brokerTopicsPath)
    zkClient.subscribeChildChanges(ZkUtils.brokerTopicsPath, topicEventListener)
  }

  class ZkTopicEventListener() extends IZkChildListener {

    @throws(classOf[Exception])
    def handleChildChange(parent: String, children: java.util.List[String]) {
      val latestTopics = children.toList

      val addedTopics = latestTopics filterNot (topics contains)
      val deletedTopics = topics filterNot (latestTopics contains)

      eventHandler match {
        case Some(handler) => {
          handler.handleTopicEvent(addedTopics, deletedTopics, latestTopics)
        }
        case None => {
          logger.debug("ignoring topic event (no event handler registered)")
        }
      }

      topics = latestTopics
    }
  }
}

