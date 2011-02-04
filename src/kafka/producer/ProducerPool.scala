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

import async.{AsyncProducerConfig, AsyncProducer}
import kafka.message.ByteBufferMessageSet
import java.util.Properties
import kafka.serializer.Encoder
import org.apache.log4j.Logger
import kafka.common.InvalidConfigException
import java.util.concurrent.{ConcurrentMap, ConcurrentHashMap}
import kafka.cluster.{Partition, Broker}

class ProducerPool[V](private val config: ProducerConfig,
                      private val serializer: Encoder[V],
                      private val syncProducers: ConcurrentMap[Int, SyncProducer],
                      private val asyncProducers: ConcurrentMap[Int, AsyncProducer[V]]) {
  private val logger = Logger.getLogger(classOf[ProducerPool[V]])
  config.producerType match {
    case "sync" =>
    case "async" =>
    case _ => throw new InvalidConfigException("Valid values for producer.type are sync/async")
  }

  def this(config: ProducerConfig, serializer: Encoder[V]) = this(config, serializer,
                                                                  new ConcurrentHashMap[Int, SyncProducer](),
                                                                  new ConcurrentHashMap[Int, AsyncProducer[V]]())
  /**
   * add a new producer, either synchronous or asynchronous, connecting
   * to the specified broker 
   * @param bid the id of the broker
   * @param host the hostname of the broker
   * @param port the port of the broker
   */
  def addProducer(broker: Broker) {
    config.producerType match {
      case "sync" =>
        val props = new Properties()
        props.put("host", broker.host)
        props.put("port", broker.port.toString)
        props.put("buffer.size", config.bufferSize.toString)
        props.put("connect.timeout.ms", config.connectTimeoutMs.toString)
        props.put("reconnect.interval", config.reconnectInterval.toString)
        val producer = new SyncProducer(new SyncProducerConfig(props))
        logger.info("Creating sync producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port)
        syncProducers.put(broker.id, producer)
      case "async" =>
        val props = new Properties()
        props.put("host", broker.host)
        props.put("port", broker.port.toString)
        props.put("serializer.class", config.serializerClass)
        val producer = new AsyncProducer[V](new AsyncProducerConfig(props))
        logger.info("Creating async producer for broker id = " + broker.id + " at " + broker.host + ":" + broker.port)
        asyncProducers.put(broker.id, producer)
    }
  }

  /**
   * selects either a synchronous or an asynchronous producer, for
   * the specified broker id and calls the send API on the selected
   * producer to publish the data to the specified broker partition
   * @param topic the topic to which the data should be published
   * @param bid the broker id
   * @param partition the broker partition id
   * @param data the data to be published
   */
  def send(topic: String, bidPid: Partition, data: V*) {
    config.producerType match {
      case "sync" =>
        logger.debug("Fetching producer for broker id: " + bidPid.brokerId + " and partition: " + bidPid.partId)
        val producer = syncProducers.get(bidPid.brokerId)
        if(producer != null)
          producer.send(topic, bidPid.partId, new ByteBufferMessageSet(data.map(d => serializer.toMessage(d)): _*))
      case "async" =>
        val producer = asyncProducers.get(bidPid.brokerId)
        if(producer != null)
          data.foreach(d => producer.send(topic, d, bidPid.partId))
    }
  }

  def send(poolData: ProducerPoolData[V]*) {
    poolData.foreach( pd => send(pd.getTopic, pd.getBidPid, pd.getData: _*))
  }

  def close() = {
    config.producerType match {
      case "sync" =>
        logger.info("Closing all sync producers")
        val iter = syncProducers.values.iterator
        while(iter.hasNext)
          iter.next.close
      case "async" =>
        logger.info("Closing all async producers")
        val iter = asyncProducers.values.iterator
        while(iter.hasNext)
          iter.next.close
    }
  }

  def getProducerPoolData(topic: String, bidPid: Partition, data: Seq[V]): ProducerPoolData[V] = {
    new ProducerPoolData[V](topic, bidPid, data)
  }

  class ProducerPoolData[V](topic: String,
                            bidPid: Partition,
                            data: Seq[V]) {
    def getTopic: String = topic
    def getBidPid: Partition = bidPid
    def getData: Seq[V] = data
  }
}
