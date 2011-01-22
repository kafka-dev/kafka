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

import java.util.concurrent._
import java.util.concurrent.atomic._
import scala.collection._
import org.apache.log4j.Logger
import kafka.cluster._
import kafka.utils._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import java.net.InetAddress
import org.I0Itec.zkclient.{IZkStateListener, IZkChildListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState

/**
 * This class handles the consumers interaction with zookeeper
 * 
 * Directories:
 * 1. Consumer id registry:
 * /consumers/[group_id]/ids[consumer_id] -> topic1,...topicN
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as an ephemeral znode
 * and puts all topics that it subscribes to as the value of the znode. The znode is deleted when the client is gone.
 * A consumer subscribes to event changes of the consumer id registry within its group.
 *
 * The consumer id is picked up from configuration, instead of the sequential id assigned by ZK. Generated sequential
 * ids are hard to recover during temporary connection loss to ZK, since it's difficult for the client to figure out
 * whether the creation of a sequential znode has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 *
 * 2. Broker node registry:
 * /brokers/[0...N] --> { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * This is a list of all present broker brokers. A unique logical node id is configured on each broker node. A broker
 * node registers itself on start-up and creates a znode with the logical node id under /brokers. The value of the znode
 * is a JSON String that contains (1) the host name and the port the broker is listening to, (2) a list of topics that
 * the broker serves, (3) a list of logical partitions assigned to each topic on the broker.
 * A consumer subscribes to event changes of the broker node registry.
 *
 * 3. Partition owner registry:
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] --> consumer_node_id
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a unique consumer
 * within a consumer group. The mapping is reestablished after each rebalancing.
 *
 * 4. Consumer offset tracking:
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] --> offset_counter_value
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 */
object ZookeeperConsumerConnector {
  val MAX_N_RETRIES = 4
  val shutdownCommand: FetchedDataChunk = new FetchedDataChunk(null, null)
}

/**
 *  JMX interface for monitoring consumer
 */
trait ZookeeperConsumerConnectorMBean {
  def getPartOwnerStats: String
  def getConsumerGroup: String
}

class ZookeeperConsumerConnector(val config: ConsumerConfig,
                                 val enableFetcher: Boolean) // for testing only
    extends ConsumerConnector with ZookeeperConsumerConnectorMBean {

  private val logger = Logger.getLogger(getClass())
  private var isShutdown = false
  private val shutdownLock = new Object
  private val rebalanceLock = new Object
  private var fetcher: Option[Fetcher] = None
  private var zkClient: ZkClient = null
  private val topicRegistry = new Pool[String, Pool[Partition, PartitionTopicInfo]]
  // queues : (topic,consumerThreadId) -> queue
  private val queues = new Pool[Tuple2[String,String], BlockingQueue[FetchedDataChunk]]
  private val scheduler = new KafkaScheduler(1, "Kafka-consumer-autocommit-", false)
  connectZk
  createFetcher
  if (config.autoCommit) {
    logger.info("starting auto committer every " + config.autoCommitIntervalMs + " ms")
    scheduler.scheduleWithRate(autoCommit, config.autoCommitIntervalMs, config.autoCommitIntervalMs)
  }

  def this(config: ConsumerConfig) = this(config, true)

  def createMessageStreams(topicCountMap: Map[String,Int]) : Map[String,List[KafkaMessageStream]] = {
    consume(topicCountMap)
  }

  // for java client
  def createMessageStreams(topicCountMap: java.util.Map[String,java.lang.Integer]):
    java.util.Map[String,java.util.List[KafkaMessageStream]] = {
    import scala.collection.JavaConversions._

    val scalaTopicCountMap: Map[String,Int] = topicCountMap.asInstanceOf[java.util.Map[String,Int]]
    val scalaReturn = consume(scalaTopicCountMap)
    val ret = new java.util.HashMap[String,java.util.List[KafkaMessageStream]]
    for ((topic, streams) <- scalaReturn) {
      var javaStreamList = new java.util.ArrayList[KafkaMessageStream]
      for (stream <- streams)
        javaStreamList.add(stream)
      ret.put(topic, javaStreamList)
    }
    ret
  }

  private def createFetcher() {
    if (enableFetcher)
      fetcher = Some(new Fetcher(config, zkClient)) 
  }

  private def connectZk() {
    logger.info("Connecting to zookeeper instance at " + config.zkConnect)
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, StringSerializer)
  }

  def shutdown() {
    shutdownLock synchronized {
      if (isShutdown)
        return
      scheduler.shutdown
      sendShudownToAllQueues
      if (zkClient != null) {
        zkClient.close()
        zkClient = null
      }
      fetcher match {
        case Some(f) => f.shutdown
        case None =>
      }
      isShutdown = true
    }
  }

  private def consume(topicCountMap: Map[String,Int]): Map[String,List[KafkaMessageStream]] = {
    logger.debug("entering consume ")
    if (topicCountMap == null)
      throw new RuntimeException("topicCountMap is null")

    val dirs = new ZKGroupDirs(config.groupId)
    var ret = new mutable.HashMap[String,List[KafkaMessageStream]]

    var consumerUuid : String = null
    config.consumerId match {
      case Some(consumerId) // for testing only 
        => consumerUuid = consumerId
      case None // generate unique consumerId automatically
        => consumerUuid = InetAddress.getLocalHost.getHostName + "-" + System.currentTimeMillis
    }
    val consumerIdString = config.groupId + "_" + consumerUuid
    val topicCount = new TopicCount(consumerIdString, topicCountMap)

    // listener to consumer and partition changes
    val loadBalancerListener = new ZKRebalancerListener(config.groupId, consumerIdString)
    registerConsumerInZK(dirs, consumerIdString, topicCount)
    zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener)

    // create a queue per topic per consumer thread
    val consumerThreadIdsPerTopic = topicCount.getConsumerThreadIdsPerTopic
    for ((topic, threadIdSet) <- consumerThreadIdsPerTopic) {
      var streamList: List[KafkaMessageStream] = Nil
      for (threadId <- threadIdSet) {
        val stream = new LinkedBlockingQueue[FetchedDataChunk](config.maxQueuedChunks)
        queues.put((topic, threadId), stream)
        streamList ::= new KafkaMessageStream(stream, config.consumerTimeoutMs)
      }           
      ret += (topic -> streamList)
      logger.debug("adding topic " + topic + " and stream to map..")
      
      // register on broker partition path changes
      val partitionPath = ZkUtils.brokerTopicsPath + "/" + topic
      ZkUtils.makeSurePersistentPathExists(zkClient, partitionPath)
      zkClient.subscribeChildChanges(partitionPath, loadBalancerListener)
    }

    // register listener for session expired event
    zkClient.subscribeStateChanges(
      new ZKSessionExpireListenner(dirs, consumerIdString, topicCount, loadBalancerListener))

    // explicitly trigger load balancing for this consumer
    loadBalancerListener.syncedRebalance
    ret
  }

  private def registerConsumerInZK(dirs: ZKGroupDirs, consumerIdString: String, topicCount: TopicCount) = {
    logger.info("begin registering consumer " + consumerIdString + " in ZK")
    ZkUtils.createEphemeralPathExpectConflict(zkClient, dirs.consumerRegistryDir + "/" + consumerIdString, topicCount.toJsonString)
    logger.info("end registering consumer " + consumerIdString + " in ZK")
  }

  private def sendShudownToAllQueues() = {
    for (queue <- queues.values) {
      logger.debug("Clearing up queue")
      queue.clear
      queue.put(ZookeeperConsumerConnector.shutdownCommand)
      logger.debug("Cleared queue and sent shutdown command")
    }
  }

  def autoCommit() {
    if(logger.isTraceEnabled)
      logger.trace("auto committing")
    commitOffsets
  }

  def commitOffsets() {
    if (zkClient == null)
      return
    for ((topic, infos) <- topicRegistry) {
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      for (info <- infos.values) {
        val newOffset = info.consumedOffset
        try {
          ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + info.partition.name,
            newOffset.toString)
        }
        catch {
          case t: Throwable =>
            // log it and let it go
            logger.warn("exception during commitOffsets: " + t + Utils.stackTrace(t))
        }
        if(logger.isDebugEnabled)
          logger.debug("Committed offset " + newOffset + " for topic " + info.topic)
      }
    }
  }

  // for JMX
  def getPartOwnerStats(): String = {
    val builder = new StringBuilder
    for ((topic, infos) <- topicRegistry) {
      builder.append("\n" + topic + ": [")
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      for(partition <- infos.values) {
        builder.append("\n    {")
        builder.append{partition.partition.name}
        builder.append(",fetch offset:" + partition.fetchedOffset)
        builder.append(",consumer offset:" + partition.consumedOffset)
        builder.append("}")
      }
      builder.append("\n        ]")
    }
    builder.toString
  }

  // for JMX
  def getConsumerGroup(): String = config.groupId

  class ZKSessionExpireListenner(val dirs: ZKGroupDirs,
                                 val consumerIdString: String,
                                 val topicCount: TopicCount,
                                 val loadBalancerListener: ZKRebalancerListener)
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
       *  connection for us. We need to release the ownership of the current consumer and re-register this
       *  consumer in the consumer registry and trigger a rebalance.
       */
      logger.info("ZK expired; release old broker parition ownership; re-register consumer " + consumerIdString)
      loadBalancerListener.resetState
      registerConsumerInZK(dirs, consumerIdString, topicCount)
      // explicitly trigger load balancing for this consumer
      loadBalancerListener.syncedRebalance

      // There is no need to resubscribe to child and state changes.
      // The child change watchers will be set inside rebalance when we read the children list. 
    }

  }

  class ZKRebalancerListener(val group: String, val consumerIdString: String)
          extends IZkChildListener {
    private val dirs = new ZKGroupDirs(group)
    private var oldPartitionsPerTopicMap: mutable.Map[String,List[String]] = new mutable.HashMap[String,List[String]]()
    private var oldConsumersPerTopicMap: mutable.Map[String,List[String]] = new mutable.HashMap[String,List[String]]()

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, curChilds : java.util.List[String]) {
      syncedRebalance
    }

    private def releasePartitionOwnership() {
      for ((topic, infos) <- topicRegistry) {
        val topicDirs = new ZKGroupTopicDirs(group, topic)
        for(partition <- infos.keys) {
          val znode = topicDirs.consumerOwnerDir + "/" + partition
          ZkUtils.deletePath(zkClient, znode)
          if(logger.isDebugEnabled)
            logger.debug("Consumer " + consumerIdString + " releasing " + znode)
        }
      }         
    }

    private def getConsumersPerTopic(group: String) : mutable.Map[String, List[String]] = {
      val consumers = ZkUtils.getChildren(zkClient, dirs.consumerRegistryDir)
      val consumersPerTopicMap = new mutable.HashMap[String, List[String]]
      for (consumer <- consumers) {
        val topicCount = getTopicCount(consumer)
        for ((topic, consumerThreadIdSet) <- topicCount.getConsumerThreadIdsPerTopic()) {
          for (consumerThreadId <- consumerThreadIdSet)
            consumersPerTopicMap.get(topic) match {
              case Some(curConsumers) => consumersPerTopicMap.put(topic, consumerThreadId :: curConsumers)
              case _ => consumersPerTopicMap.put(topic, List(consumerThreadId))
            }
        }
      }
      for ( (topic, consumerList) <- consumersPerTopicMap )
        consumersPerTopicMap.put(topic, consumerList.sortWith((s,t) => s < t))
      consumersPerTopicMap
    }

    private def getRelevantTopicMap(myTopicThreadIdsMap: Map[String, Set[String]],
                                    newPartMap: Map[String,List[String]],
                                    oldPartMap: Map[String,List[String]],
                                    newConsumerMap: Map[String,List[String]],
                                    oldConsumerMap: Map[String,List[String]]): Map[String, Set[String]] = {
      var relevantTopicThreadIdsMap = new mutable.HashMap[String, Set[String]]()
      for ( (topic, consumerThreadIdSet) <- myTopicThreadIdsMap )
        if ( oldPartMap.get(topic) != newPartMap.get(topic) || oldConsumerMap.get(topic) != newConsumerMap.get(topic))
          relevantTopicThreadIdsMap += (topic -> consumerThreadIdSet)
      relevantTopicThreadIdsMap
    }

    private def getTopicCount(consumerId: String) : TopicCount = {
      val topicCountJson = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)
      TopicCount.constructTopicCount(consumerId, topicCountJson)
    }

    def resetState() {
      topicRegistry.clear
      oldConsumersPerTopicMap.clear
      oldPartitionsPerTopicMap.clear
    }

    def syncedRebalance() {
      rebalanceLock synchronized {
        for (i <- 0 until ZookeeperConsumerConnector.MAX_N_RETRIES) {
          logger.info("begin rebalancing consumer " + consumerIdString + " try #" + i)
          val done = rebalance
          logger.info("end rebalancing consumer " + consumerIdString + " try #" + i)
          if (done)
            return
          // release all partitions, reset state and retry
          releasePartitionOwnership
          resetState
          Thread.sleep(config.zkSyncTimeMs)
        }
      }

      throw new RuntimeException(consumerIdString + " can't rebalance after " + ZookeeperConsumerConnector.MAX_N_RETRIES +" retires")
    }

    private def rebalance(): Boolean = {
      // testing code
      //if ("group1_consumer1" == consumerIdString) {
      //  logger.info("sleeping " + consumerIdString)
      //  Thread.sleep(20)
      //}

      val myTopicThreadIdsMap = getTopicCount(consumerIdString).getConsumerThreadIdsPerTopic
      val cluster = ZkUtils.getCluster(zkClient)
      val consumersPerTopicMap = getConsumersPerTopic(group)
      val partitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, myTopicThreadIdsMap.keys.iterator)
      val relevantTopicThreadIdsMap = getRelevantTopicMap(myTopicThreadIdsMap, partitionsPerTopicMap, oldPartitionsPerTopicMap, consumersPerTopicMap, oldConsumersPerTopicMap)
      if (relevantTopicThreadIdsMap.size <= 0) {
        logger.info("Consumer " + consumerIdString + " with " + consumersPerTopicMap + " doesn't need to rebalance.")
        return true
      }

      logger.info("Committing all offsets")
      commitOffsets

      logger.info("Releasing partition ownership")
      releasePartitionOwnership


      for ((topic, consumerThreadIdSet) <- relevantTopicThreadIdsMap) {
        topicRegistry.remove(topic)
        topicRegistry.put(topic, new Pool[Partition, PartitionTopicInfo])

        val topicDirs = new ZKGroupTopicDirs(group, topic)
        val curConsumers = consumersPerTopicMap.get(topic).get
        var curPartitions: List[String] = partitionsPerTopicMap.get(topic).get

        val nPartsPerConsumer = curPartitions.size / curConsumers.size
        val nConsumersWithExtraPart = curPartitions.size % curConsumers.size

        logger.info("Consumer " + consumerIdString + " rebalancing the following partitions: " + curPartitions + " for topic " + topic + " with consumers: " + curConsumers)

        for (consumerThreadId <- consumerThreadIdSet) {
          val myConsumerPosition = curConsumers.findIndexOf(_ == consumerThreadId)
          assert(myConsumerPosition >= 0)
          val startPart = nPartsPerConsumer*myConsumerPosition + myConsumerPosition.min(nConsumersWithExtraPart)
          val nParts = nPartsPerConsumer + (if (myConsumerPosition + 1 > nConsumersWithExtraPart) 0 else 1)

          /**
           *   Range-partition the sorted partitions to consumers for better locality.
           *  The first few consumers pick up an extra partition, if any.
           */
          if (nParts <= 0)
            logger.warn("No broker partions consumed by consumer thread " + consumerThreadId + " for topic " + topic)
          for (i <- startPart until startPart + nParts) {
            val partition = curPartitions(i)
            logger.info(consumerThreadId + " attempting to claim partition " + partition)
            if (!processPartition(topicDirs, partition, topic, consumerThreadId))
              return false
          }
        }
      }      
      updateFetcher(cluster)
      oldPartitionsPerTopicMap = partitionsPerTopicMap
      oldConsumersPerTopicMap = consumersPerTopicMap
      true
    }

    private def updateFetcher(cluster: Cluster) {
      // update partitions for fetcher
      var allPartitionInfos : List[PartitionTopicInfo] = Nil
      for (partitionInfos <- topicRegistry.values)
        for (partition <- partitionInfos.values)
          allPartitionInfos ::= partition
      logger.info("Consumer " + consumerIdString + " selected partitions : " +
              allPartitionInfos.sortWith((s,t) => s.partition < t.partition).map(_.toString).mkString(","))

      fetcher match {
        case Some(f) => f.initConnections(allPartitionInfos, cluster)
        case None =>
      }
    }

    private def processPartition(topicDirs: ZKGroupTopicDirs, partition: String,
                                 topic: String, consumerThreadId: String) : Boolean = {
      val partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition
      try {
        ZkUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId)
      }
      catch {
        case e: ZkNodeExistsException =>
          // The node hasn't been deleted by the original owner. So wait a bit and retry.
          logger.info("waiting for the partition ownership to be deleted: " + partition)
          return false
        case e2 => throw e2
      }
      addPartitionTopicInfo(topicDirs, partition, topic, consumerThreadId)
      true
    }

    private def addPartitionTopicInfo(topicDirs: ZKGroupTopicDirs, partitionString: String,
                                      topic: String, consumerThreadId: String) {
      val partition = Partition.parse(partitionString)
      val partitionTopicInfo = topicRegistry.get(topic)

      val znode = topicDirs.consumerOffsetDir + "/" + partition.name
      val offsetString = ZkUtils.readDataMaybeNull(zkClient, znode)
      // If first time starting a consumer, use default offset.
      // TODO: handle this better (if client doesn't know initial offsets)
      val offset : Long = if (offsetString == null) 0 else offsetString.toLong
      val queue = queues.get((topic, consumerThreadId))
      val consumedOffset = new AtomicLong(offset)
      val fetchedOffset = new AtomicLong(offset)
      partitionTopicInfo.put(partition,
                             new PartitionTopicInfo(topic,
                                                    partition.brokerId,
                                                    partition,
                                                    queue,
                                                    consumedOffset,
                                                    fetchedOffset,
                                                    new AtomicInteger(config.fetchSize)))
    }
  }
}
