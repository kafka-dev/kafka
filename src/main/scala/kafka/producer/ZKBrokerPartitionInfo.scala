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

import kafka.utils.{StringSerializer, ZkUtils, ZKConfig}
import collection.mutable.HashMap
import collection.mutable.Map
import collection.SortedSet
import org.apache.log4j.Logger
import collection.immutable.TreeSet
import kafka.cluster.{Broker, Partition}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener, ZkClient}

private[producer] object ZKBrokerPartitionInfo {
  private val log = Logger.getLogger(classOf[ZKBrokerPartitionInfo])
  /**
   * Generate a mapping from broker id to (brokerId, numPartitions) for the list of brokers
   * specified
   * @param topic the topic to which the brokers have registered
   * @param brokerList the list of brokers for which the partitions info is to be generated
   * @return a sequence of (brokerId, numPartitions) for brokers in brokerList
   */
  private def getBrokerPartitions(zkClient: ZkClient, topic: String, brokerList: List[Int]): SortedSet[Partition] = {
    val brokerTopicPath = ZkUtils.brokerTopicsPath + "/" + topic
    val numPartitions = brokerList.map(bid => ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid).toInt)
    val brokerPartitions = brokerList.zip(numPartitions)

    val sortedBrokerPartitions = brokerPartitions.sortWith((id1, id2) => id1._1 < id2._1)

    var brokerParts = SortedSet.empty[Partition]
    sortedBrokerPartitions.foreach { bp =>
      for(i <- 0 until bp._2) {
        val bidPid = new Partition(bp._1, i)
        brokerParts = brokerParts + bidPid
      }
    }
    log.debug("Sorted list of broker ids = " + brokerParts.toString)
    brokerParts
  }  
}

/**
 * If zookeeper based auto partition discovery is enabled, fetch broker info like
 * host, port, number of partitions from zookeeper
 */
private[producer] class ZKBrokerPartitionInfo(config: ZKConfig, producerCbk: (Int, String, Int) => Unit) extends BrokerPartitionInfo {
  private val logger = Logger.getLogger(classOf[ZKBrokerPartitionInfo])
  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
    StringSerializer)
  // maintain a map from topic -> list of (broker, num_partitions) from zookeeper
  private var topicBrokerPartitions = getZKTopicPartitionInfo

  private val topicBrokersListener = new TopicBrokersListener(topicBrokerPartitions)

  // register listener for change of topics to keep topicsBrokerPartitions updated
  private val topicsListener = new TopicsListener(topicBrokerPartitions, topicBrokersListener)
  zkClient.subscribeChildChanges(ZkUtils.brokerTopicsPath, topicsListener)

  // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
  topicBrokerPartitions.keySet.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.brokerTopicsPath + "/" + topic,
                                                topicBrokersListener))
  private var allBrokers = getZKBrokerInfo

  // register listener for new broker
  private val brokerListener = new BrokerListener(allBrokers.keySet.toSeq)
  zkClient.subscribeChildChanges(ZkUtils.brokerIdsPath, brokerListener)

  // register listener for session expired event
  zkClient.subscribeStateChanges(new ZKSessionExpirationListener(brokerListener, topicBrokersListener, topicsListener))

  /**
   * Return a sequence of (brokerId, numPartitions)
   * @param topic the topic for which this information is to be returned
   * @return a sequence of (brokerId, numPartitions)
   */
  def getBrokerPartitionInfo(topic: String): SortedSet[Partition] = {
    val brokerPartitions = topicBrokerPartitions.get(topic)
    var numBrokerPartitions = SortedSet.empty[Partition]
    brokerPartitions match {
      case Some(bp) => numBrokerPartitions = TreeSet[Partition]() ++ brokerPartitions.get
      case None =>  // no brokers currently registered for this topic. Find the list of all brokers in the cluster.
        val allBrokersIds = ZkUtils.getChildren(zkClient, ZkUtils.brokerIdsPath)
        // since we do not have the in formation about number of partitions on these brokers, just assume single partition
        // i.e. pick partition 0 from each broker as a candidate
        numBrokerPartitions = TreeSet[Partition]() ++ allBrokersIds.map(b => new Partition(b.toInt, 0))
    }
    numBrokerPartitions
  }

  /**
   * Generate the host and port information for the broker identified
   * by the given broker id
   * @param brokerId the broker for which the info is to be returned
   * @return host and port of brokerId
   */
  def getBrokerInfo(brokerId: Int): Option[Broker] =  allBrokers.get(brokerId)

  /**
   * Generate a mapping from broker id to the host and port for all brokers
   * @return mapping from id to host and port of all brokers
   */
  def getAllBrokerInfo: Map[Int, Broker] = allBrokers

  def close = zkClient.close
  
  /**
   * Generate a sequence of (brokerId, numPartitions) for all topics
   * registered in zookeeper
   * @return a mapping from topic to sequence of (brokerId, numPartitions)
   */
  private def getZKTopicPartitionInfo(): collection.mutable.Map[String, SortedSet[Partition]] = {
    val brokerPartitionsPerTopic = new HashMap[String, SortedSet[Partition]]()
    ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.brokerTopicsPath)
    val topics = ZkUtils.getChildren(zkClient, ZkUtils.brokerTopicsPath)
    topics.foreach { topic =>
    // find the number of broker partitions registered for this topic
      val brokerTopicPath = ZkUtils.brokerTopicsPath + "/" + topic
      val brokerList = ZkUtils.getChildren(zkClient, brokerTopicPath)
      val numPartitions = brokerList.map(bid => ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid).toInt)
      val brokerPartitions = brokerList.map(bid => bid.toInt).zip(numPartitions)
      val sortedBrokerPartitions = brokerPartitions.sortWith((id1, id2) => id1._1 < id2._1)
      logger.debug("Sorted list of broker ids for topic: " + topic + " = " + sortedBrokerPartitions.toString)

      var brokerParts = SortedSet.empty[Partition]
      sortedBrokerPartitions.foreach { bp =>
        for(i <- 0 until bp._2) {
          val bidPid = new Partition(bp._1, i)
          brokerParts = brokerParts + bidPid
        }
      }
      brokerPartitionsPerTopic += (topic -> brokerParts)
    }
    brokerPartitionsPerTopic
  }

  /**
   * Generate a mapping from broker id to (brokerId, numPartitions) for all brokers
   * registered in zookeeper
   * @return a mapping from brokerId to (host, port)
   */
  private def getZKBrokerInfo(): Map[Int, Broker] = {
    val brokers = new HashMap[Int, Broker]()
    val allBrokerIds = ZkUtils.getChildren(zkClient, ZkUtils.brokerIdsPath).map(bid => bid.toInt)
    allBrokerIds.foreach { bid =>
      val brokerInfo = ZkUtils.readData(zkClient, ZkUtils.brokerIdsPath + "/" + bid)
      brokers += (bid -> Broker.createBroker(bid, brokerInfo))
    }
    brokers
  }

  /**
   * Listens to new topic registrations in zookeeper and keeps the related data structures updated
   */
  class TopicsListener(val originalTopicBrokerPartitionsMap: collection.mutable.Map[String, SortedSet[Partition]],
                       val topicBrokersListener: TopicBrokersListener)
          extends IZkChildListener {
    private var oldTopicBrokerPartitionsMap = originalTopicBrokerPartitionsMap

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      // check if event is for new topic
      import scala.collection.JavaConversions._
      // check to see if this event indicates new topic or a newly registered broker for an existing topic
      logger.debug("[TopicsListener] Path changed at " + parentPath + " with updated children -> " +
              curChilds.toString)
      processNewTopic(asBuffer(curChilds))
    }

    /**
     * Generate a mapping from broker id to (brokerId, numPartitions) for the list of brokers
     * registered under the specified topics
     * @param updatedTopics the list of new topics in zookeeper
     */
    def processNewTopic(updatedTopics: Seq[String]) = {
      logger.debug("[TopicsListener] Old list of topics: " + oldTopicBrokerPartitionsMap.keySet.toString)
      logger.debug("[TopicsListener] Updated list of topics: " + updatedTopics.toSet.toString)
      val newTopics = updatedTopics.toSet &~ oldTopicBrokerPartitionsMap.keySet
      logger.debug("[TopicsListener] New list of topics: " + newTopics.toString)
      newTopics.foreach { topic =>
        // find the number of broker partitions registered for this topic
        val brokerTopicPath = ZkUtils.brokerTopicsPath + "/" + topic
        val brokerList = ZkUtils.getChildren(zkClient, brokerTopicPath)
        import ZKBrokerPartitionInfo._
        val brokerParts = getBrokerPartitions(zkClient, topic, brokerList.map(b => b.toInt).toList)
        logger.debug("[TopicsListener] List of broker partitions for new topic " + topic + " are " + brokerParts.toString)
        topicBrokerPartitions += (topic -> brokerParts)
        zkClient.subscribeChildChanges(ZkUtils.brokerTopicsPath + "/" + topic,
                                       topicBrokersListener)
      }
    }

    def resetState = {
      oldTopicBrokerPartitionsMap = topicBrokerPartitions
    }
  }

  /**
   * Listens to new broker registrations under a particular topic, in zookeeper and
   * keeps the related data structures updated
   */
  class TopicBrokersListener(val originalTopicBrokerPartitionsMap: collection.mutable.Map[String, SortedSet[Partition]])
          extends IZkChildListener {
    private var oldTopicBrokerPartitionsMap = originalTopicBrokerPartitionsMap

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      // check if event is for new topic
      import scala.collection.JavaConversions._
      val topic = parentPath.split("/").last
      // check to see if this event indicates new topic or a newly registered broker for an existing topic
      logger.debug("[TopicBrokersListener] Path changed at " + parentPath + " with updated children -> " +
              curChilds.toString + " for topic -> " + topic)

      processNewBrokerInExistingTopic(parentPath, asBuffer(curChilds))
    }

    /**                        
     * Generate the updated mapping of (brokerId, numPartitions) for the new list of brokers
     * registered under some topic
     * @param parentPath the path of the topic under which the brokers have changed
     * @param curChilds the list of changed brokers
     */
    def processNewBrokerInExistingTopic(parentPath: String, curChilds: Seq[String]) = {
      val topic = parentPath.split("/").last
      val updatedBrokerList = curChilds.map(b => b.toInt)
      import ZKBrokerPartitionInfo._
      val brokerParts = getBrokerPartitions(zkClient, topic, updatedBrokerList.toList)
      logger.debug("[BrokerListener] Updated list of brokers: " + curChilds.toString)
      topicBrokerPartitions += (topic -> brokerParts)
      // find the old list of brokers for this topic
      oldTopicBrokerPartitionsMap.get(topic) match {
        case Some(brokersParts) =>
          logger.debug("[BrokerListener] Old list of brokers: " + brokersParts.map(bp => bp.brokerId).toString)
        case None =>
      }
      logger.debug("[TopicBrokersListener] List of broker partitions for topic " + topic + " are " + brokerParts.toString)
    }

    def resetState = {
      oldTopicBrokerPartitionsMap = topicBrokerPartitions
    }
  }
  /**
   * Listens to new broker registrations in zookeeper and keeps the related data structures updated
   */
  class BrokerListener(val brokerList: Seq[Int])
          extends IZkChildListener {
    private var oldBrokerIds = brokerList

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      logger.debug("[BrokerListener] Path changed at " + parentPath + " with updated children -> " + curChilds.toString)
      logger.debug("[BrokerListener] Old list of brokers: " + oldBrokerIds.toSet.toString)
      import scala.collection.JavaConversions._
      handleBrokerChange(parentPath, asBuffer(curChilds))
    }

    def handleBrokerChange(parentPath: String, curChilds: Seq[String]) {
      if(parentPath.equals(ZkUtils.brokerIdsPath)) {
        import scala.collection.JavaConversions._
        val updatedBrokerList = asBuffer(curChilds).map(bid => bid.toInt)
        val newBrokers = updatedBrokerList.toSet &~ oldBrokerIds.toSet
        logger.debug("[BrokerListener] New list of brokers: " + newBrokers.toString)
        newBrokers.foreach { bid =>
          val brokerInfo = ZkUtils.readData(zkClient, ZkUtils.brokerIdsPath + "/" + bid)
          val brokerHostPort = brokerInfo.split(":")
          allBrokers += (bid -> new Broker(bid, brokerHostPort(1), brokerHostPort(1), brokerHostPort(2).toInt))
          logger.debug("Invoking the callback for broker: " + bid)
          producerCbk(bid, brokerHostPort(1), brokerHostPort(2).toInt)
        }
      }
    }

    def resetState = {
      oldBrokerIds = allBrokers.keySet.toSeq
    }
  }

  /**
   * Handles the session expiration event in zookeeper
   */
  class ZKSessionExpirationListener(val brokerListener: BrokerListener,
                                 val topicBrokersListener: TopicBrokersListener,
                                 val topicsListener: TopicsListener)
          extends IZkStateListener {
    @throws(classOf[Exception])
    def handleStateChanged(state: KeeperState) {
      // do nothing, since zkclient will do reconnect for us.
    }

    /**
     * Called after the zookeeper session has expired and a new session has been created. You would have to re-create
     * any ephemeral nodes here.
     *
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleNewSession() {
      /**
       *  When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has reestablished a
       *  connection for us.
       */
      logger.info("ZK expired; release old list of broker partitions for topics ")
      topicBrokerPartitions = getZKTopicPartitionInfo
      allBrokers = getZKBrokerInfo
      brokerListener.resetState
      topicBrokersListener.resetState
      topicsListener.resetState

      // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
      // NOTE: this is probably not required here. Since when we read from getZKTopicPartitionInfo() above,
      // it automatically recreates the watchers there itself
      topicBrokerPartitions.keySet.foreach(topic => zkClient.subscribeChildChanges(ZkUtils.brokerTopicsPath + "/" + topic,
                                                topicBrokersListener))
      // there is no need to re-register other listeners as they are listening on the child changes of
      // permanent nodes
    }

  }

}