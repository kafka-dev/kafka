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
  /** consumer group ID */
  val groupId = Utils.getString(props, "groupid")

  /** consumer id: generated automatically if not set.
   *  Set this explicitly for only testing purpose. */
  val consumerId: Option[String] = /** TODO: can be written better in scala 2.8 */
    if (Utils.getString(props, "consumerid", null) != null) Some(Utils.getString(props, "consumerid")) else None

  /** the socket timeout for network requests */
  val socketTimeoutMs = Utils.getInt(props, "zk.session.timeoutms", 30 * 1000)
  
  /** the socket receive buffer for network requests */
  val socketBufferSize = Utils.getInt(props, "socket.buffersize", 64*1024)
  
  /** the number of byes of messages to attempt to fetch */
  val fetchSize = Utils.getInt(props, "fetch.size", 300 * 1024)
  
  /** the maximum allowable fetch size for a very large message */
  val maxFetchSize: Int = fetchSize * 10
  
  /** to avoid repeatedly polling a broker node which has no new data
      we will backoff every time we get an incomplete response from
      that broker (i.e. fewer than fetchSize bytes). Each successive incomplete request
      increments the backoff by this amount */
  val backoffIncrementMs: Long = Utils.getInt(props, "backoff.incrementms", 1000)
  
  /** if true automatically commit any message set fetched without waiting for confirmation
      from the consumer of processing */
  val autoCommit = Utils.getBoolean(props, "autocommit.enable", true)
  
  /** auto commit interval */
  val autoCommitIntervalMs = Utils.getInt(props, "autocommit.intervalms", 10 * 1000)

  /** max fetched chunks in a queue */
  val maxQueuedChunks = Utils.getInt(props, "queuedchunks.max", 100)

  /* auto offset reset {smallest, largest} */
  val autoOffsetReset = Utils.getString(props, "autooffset.reset", OffsetRequest.SMALLEST_TIME_STRING)

  /** timeout for consumer returned from consumer connector */
  val consumerTimeoutMs = Utils.getInt(props, "consumer.timeoutms", -1)

  /* topics map for the embedded consumer. The format is topic1:1,topic2:1 */
  val embeddedConsumerTopics = Utils.getString(props, "embeddedconsumer.topics", null)
}
