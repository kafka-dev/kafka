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

import async.{QueueItem, CallbackHandler, IEventHandler}
import org.apache.log4j.Logger
import kafka.serializer.Encoder
import kafka.utils._
import kafka.common.InvalidConfigException
import java.util.Properties
import kafka.cluster.{Partition, Broker}
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.InvalidPartitionException

class Producer[K,V](config: ProducerConfig,
                    partitioner: Partitioner[K],
                    producerPool: ProducerPool[V],
                    populateProducerPool: Boolean = true) /* for testing purpose only. Applications should ideally */
                                                          /* use the other constructor*/
{
  private val logger = Logger.getLogger(classOf[Producer[K, V]])
  private val hasShutdown = new AtomicBoolean(false)
  if(!Utils.propertyExists(config.zkConnect) && !Utils.propertyExists(config.brokerPartitionInfo))
    throw new InvalidConfigException("At least one of zk.connect or broker.partition.info must be specified")
  private val random = new java.util.Random
  private var brokerPartitionInfo: BrokerPartitionInfo = null
  // check if zookeeper based auto partition discovery is enabled
  private val zkEnabled = if(!Utils.propertyExists(config.zkConnect)) false else true
  zkEnabled match {
    case true =>
      val zkProps = new Properties()
      zkProps.put("zk.connect", config.zkConnect)
      zkProps.put("zk.sessiontimeout.ms", config.zkSessionTimeoutMs.toString)
      zkProps.put("zk.connectiontimeout.ms", config.zkConnectionTimeoutMs.toString)
      zkProps.put("zk.synctime.ms", config.zkSyncTimeMs.toString)
      brokerPartitionInfo = new ZKBrokerPartitionInfo(new ZKConfig(zkProps), producerCbk)
    case false =>
      brokerPartitionInfo = new ConfigBrokerPartitionInfo(config)
  }

  // pool of producers, one per broker
  if(populateProducerPool) {
    val allBrokers = brokerPartitionInfo.getAllBrokerInfo
    allBrokers.foreach(b => producerPool.addProducer(new Broker(b._1, b._2.host, b._2.host, b._2.port)))
  }

  def this(config: ProducerConfig) =  this(config, Utils.getObject(config.partitionerClass),
    new ProducerPool[V](config, Utils.getObject(config.serializerClass)))

  def this(config: ProducerConfig,
           eventHandler: IEventHandler[V],
           cbkHandler: CallbackHandler[V],
           partitioner: Partitioner[K]) =
    this(config, partitioner,
         new ProducerPool[V](config, Utils.getObject(config.serializerClass), eventHandler, cbkHandler))

  /**
   * Sends the data, partitioned by key to the topic using either the
   * synchronous or the asynchronous producer 
   * @param topic the topic under which the message is to be published
   * @param key the key used by the partitioner to pick a broker partition
   * @param data the data to be published as Kafka messages under topic
   */
  def send(producerData: ProducerData[K,V]*) {
    val producerPoolRequests = producerData.map { pd =>
      // find the number of broker partitions registered for this topic
      val numBrokerPartitions = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic).toSeq
      val totalNumPartitions = numBrokerPartitions.length
      // get the partition id
      val partitionId = getPartition(pd.getKey, totalNumPartitions)
      val brokerIdPartition = numBrokerPartitions(partitionId)
      val brokerInfo = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.brokerId).get
      logger.debug("Sending message to broker " + brokerInfo.host + ":" + brokerInfo.port +
              " on partition " + brokerIdPartition.partId)
      producerPool.getProducerPoolData(pd.getTopic,
                                       new Partition(brokerIdPartition.brokerId, brokerIdPartition.partId),
                                       pd.getData)
    }
    producerPool.send(producerPoolRequests: _*)
  }

  /**
   * Retrieves the partition id and throws an InvalidPartitionException if
   * the value of partition is not between 0 and numPartitions-1
   * @param key the partition key
   * @param numPartitions the total number of available partitions
   * @returns the partition id
   */
  private def getPartition(key: K, numPartitions: Int): Int = {
    if(numPartitions <= 0)
      throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions +
              "\n Valid values are > 0")
    val partition = if(key == null) random.nextInt(numPartitions)
                    else partitioner.partition(key , numPartitions)
    if(partition < 0 || partition >= numPartitions)
      throw new InvalidPartitionException("Invalid partition id : " + partition +
              "\n Valid values are in the range inclusive [0, " + (numPartitions-1) + "]")
    partition
  }
  
  /**
   * Callback to add a new producer to the producer pool. Used by ZKBrokerPartitionInfo
   * on registration of new broker in zookeeper
   * @param bid the id of the broker
   * @param host the hostname of the broker
   * @param port the port of the broker
   */
  private def producerCbk(bid: Int, host: String, port: Int) =  {
    if(populateProducerPool) producerPool.addProducer(new Broker(bid, host, host, port))
    else logger.debug("Skipping the callback..")
  }

  def close() = {
    val canShutdown = hasShutdown.compareAndSet(false, true)
    if(canShutdown) {
      producerPool.close
      brokerPartitionInfo.close
    }
  }
}