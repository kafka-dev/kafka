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

class RichProducerConfig(props: Properties) extends ProducerConfig(props)  {
  /** the partitioner class for partitioning events amongst sub-topics */
  val partitionerClass = Utils.getString(props, "partitioner.class")

  /** ZK host string */
  val zkConnect = Utils.getString(props, "zk.connect", "127.0.0.1:2182")

  /** zookeeper session timeout */
  val zkSessionTimeoutMs = Utils.getInt(props, "zk.sessiontimeout.ms", 6000)

  /** the max time that the client waits to establish a connection to zookeeper */
  val zkConnectionTimeoutMs = Utils.getInt(props, "zk.connectiontimeout.ms", zkSessionTimeoutMs)

  /** how far a ZK follower can be behind a ZK leader */
  val zkSyncTimeMs = Utils.getInt(props, "zk.synctime.ms", 2000)
}