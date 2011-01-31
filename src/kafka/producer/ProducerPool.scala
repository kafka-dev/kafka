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
import collection.mutable.HashMap
import collection.mutable.Map
import org.apache.log4j.Logger
import kafka.common.InvalidConfigException

class ProducerPool[V](private val config: ProducerConfig,
                      private val serializer: Encoder[V],
                      private val allBrokers: Map[Int, (String, Int)]) {
  private val logger = Logger.getLogger(classOf[ProducerPool[V]])
  config.producerType match {
    case "sync" =>
    case "async" =>
    case _ => throw new InvalidConfigException("Valid values for producer.type are sync/async")
  }
  private val syncProducers = new HashMap[Int, SyncProducer]()
  private val asyncProducers = new HashMap[Int, AsyncProducer[V]]()

  allBrokers.foreach(b => addProducer(b._1, b._2._1, b._2._2))

  /**
   * add a new producer, either synchronous or asynchronous, connecting
   * to the specified broker 
   * @param bid the id of the broker
   * @param host the hostname of the broker
   * @param port the port of the broker
   */
  def addProducer(bid: Int, host: String, port: Int) {
    config.producerType match {
      case "sync" =>
        val props = new Properties()
        props.put("host", host)
        props.put("port", port.toString)
        props.put("buffer.size", config.bufferSize.toString)
        props.put("connect.timeout.ms", config.connectTimeoutMs.toString)
        props.put("reconnect.interval", config.reconnectInterval.toString)
        val producer = new SyncProducer(new SyncProducerConfig(props))
        logger.info("Creating sync producer for broker id = " + bid + " at " + host + ":" + port)
        syncProducers += (bid -> producer)
      case "async" =>
        val props = new Properties()
        props.put("host", host)
        props.put("port", port.toString)
        props.put("serializer.class", config.serializerClass)
        val producer = new AsyncProducer[V](new AsyncProducerConfig(props))
        logger.info("Creating async producer for broker id = " + bid + " at " + host + ":" + port)
        asyncProducers += (bid -> producer)
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
  def send(topic: String, bid: Int, partition: Int, data: V*) {
    config.producerType match {
      case "sync" =>
        val producer = syncProducers.get(bid).get
        producer.send(topic, partition, new ByteBufferMessageSet(data.map(d => serializer.toMessage(d)): _*))
      case "async" =>
        val producer = asyncProducers.get(bid).get
        data.foreach(d => producer.send(topic, d, partition))
    }
  }
}
