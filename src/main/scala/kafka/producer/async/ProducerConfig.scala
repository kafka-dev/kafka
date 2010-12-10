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

package kafka.producer.async

import kafka.utils.Utils
import java.util.Properties

class ProducerConfig(props: Properties) {

  /** the broker to which the producer sends events */
  val host = Utils.getString(props, "host")

  /** the port on which the broker is running */
  val port = Utils.getInt(props, "port")

  val bufferSize = Utils.getInt(props, "buffer.size", 100*1024)

  val connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000)

  val reconnectInterval = Utils.getInt(props, "reconnect.interval", 30000)

  /* maximum time, in milliseconds, for buffering data on the producer queue */
  val queueTime = Utils.getInt(props, "queue.time", 5000)
  
  /** the maximum size of the blocking queue for buffering on the producer */
  val queueSize = Utils.getInt(props, "queue.size", 10000)

  /** the number of messages batched at the producer */
  val batchSize = Utils.getInt(props, "batch.size", 200)

  /** the serializer class for events */
  val serializerClass = Utils.getString(props, "serializer.class")
  
}