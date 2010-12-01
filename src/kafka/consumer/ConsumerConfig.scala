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

import java.util.Properties
import kafka.utils.{ZKConfig, Utils}
import kafka.api.OffsetRequest

class ConsumerConfig(props: Properties) extends ZKConfig(props) {
  /** a string that uniquely identifies a set of consumers within the same consumer group */
  val groupId = Utils.getString(props, "groupid")

  /** consumer id: generated automatically if not set.
   *  Set this explicitly for only testing purpose. */
  val consumerId: Option[String] = /** TODO: can be written better in scala 2.8 */
    if (Utils.getString(props, "consumerid", null) != null) Some(Utils.getString(props, "consumerid")) else None

  /** the socket timeout for network requests */
  val socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", 30 * 1000)
  
  /** the socket receive buffer for network requests */
  val socketBufferSize = Utils.getInt(props, "socket.buffersize", 64*1024)
  
  /** the number of byes of messages to attempt to fetch */
  val fetchSize = Utils.getInt(props, "fetch.size", 300 * 1024)
  
  /** the maximum allowable fetch size for a very large message */
  val maxFetchSize: Int = fetchSize * 10
  
  /** to avoid repeatedly polling a broker node which has no new data
      we will backoff every time we get an empty set from the broker*/
  val backoffIncrementMs: Long = Utils.getInt(props, "backoff.increment.ms", 1000)
  
  /** if true, periodically commit to zookeeper the offset of messages already fetched by the consumer */
  val autoCommit = Utils.getBoolean(props, "autocommit.enable", true)
  
  /** the frequency in ms that the consumer offsets are committed to zookeeper */
  val autoCommitIntervalMs = Utils.getInt(props, "autocommit.interval.ms", 10 * 1000)

  /** max number of messages buffered for consumption */
  val maxQueuedChunks = Utils.getInt(props, "queuedchunks.max", 100)

  /* what to do if an offset is out of range.
     smallest : automatically reset the offset to the smallest offset
     largest : automatically reset the offset to the largest offset
     anything else: throw exception to the consumer */
  val autoOffsetReset = Utils.getString(props, "autooffset.reset", OffsetRequest.SMALLEST_TIME_STRING)

  /** throw a timeout exception to the consumer if no message is available for consumption after the specified interval */
  val consumerTimeoutMs = Utils.getInt(props, "consumer.timeout.ms", -1)

  /* embed a consumer in the broker. e.g., topic1:1,topic2:1 */
  val embeddedConsumerTopics = Utils.getString(props, "embeddedconsumer.topics", null)
}
