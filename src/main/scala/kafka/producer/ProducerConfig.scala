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

import async.AsyncProducerConfig
import java.util.Properties
import kafka.utils.Utils

class ProducerConfig(props: Properties) {

  /** the partitioner class for partitioning events amongst sub-topics */
  val partitionerClass = Utils.getString(props, "partitioner.class", "kafka.producer.DefaultPartitioner")

  /** the serializer class for converting data to kafka messages */
  val serializerClass = Utils.getString(props, "serializer.class")
  
  /** this parameter specifies whether the messages are sent asynchronously *
   * or not. Valid values are - async for asynchronous send                 *
   *                            sync for synchronous send                   */
  val producerType = Utils.getString(props, "producer.type", "sync")

  /** For bypassing zookeeper based auto partition discovery, use this config   *
   *  to pass in static broker and per-broker partition information. Format-    *
   *  brokerid1:host1:port1:numPartitions1, brokerid2:host2:port2:numPartitions2*/
  val brokerPartitionInfo = Utils.getString(props, "broker.partition.info", null)

  /** To enable zookeeper based auto partition discovery, specify the ZK    *
   * connection string. This will override the brokerPartitionInfo, in case *
   * both are specified by the user                                         */
  val zkConnect = Utils.getString(props, "zk.connect", null)

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms", zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000)

  /** Async Producer config options */
  /* maximum time, in milliseconds, for buffering data on the producer queue */
  val queueTime = Utils.getInt(props, "queue.time", 5000)

  /** the maximum size of the blocking queue for buffering on the producer */
  val queueSize = Utils.getInt(props, "queue.size", 10000)

  /** the number of messages batched at the producer */
  val batchSize = Utils.getInt(props, "batch.size", 200)

  /** the callback handler for one or multiple events */
  val cbkHandler = Utils.getString(props, "callback.handler", null)

  /** properties required to initialize the callback handler */
  val cbkHandlerProps = Utils.getProps(props, "callback.handler.props", null)

  /** the handler for events */
  val eventHandler = Utils.getString(props, "event.handler", null)

  /** properties required to initialize the callback handler */
  val eventHandlerProps = Utils.getProps(props, "event.handler.props", null)

  /** Sync Producer config options */
  val bufferSize = Utils.getInt(props, "buffer.size", 100*1024)

  val connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000)

  val reconnectInterval = Utils.getInt(props, "reconnect.interval", 30000)

}