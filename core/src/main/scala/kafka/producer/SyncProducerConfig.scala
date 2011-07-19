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

import kafka.utils.Utils
import java.util.Properties
import kafka.message.{CompressionUtils, CompressionCodec}

class SyncProducerConfig(val props: Properties) extends SyncProducerConfigShared {
  /** the broker to which the producer sends events */
  val host = Utils.getString(props, "host")

  /** the port on which the broker is running */
  val port = Utils.getInt(props, "port")
}

trait SyncProducerConfigShared {
  val props: Properties
  
  val bufferSize = Utils.getInt(props, "buffer.size", 100*1024)

  val connectTimeoutMs = Utils.getInt(props, "connect.timeout.ms", 5000)

  /** the socket timeout for network requests */
  val socketTimeoutMs = Utils.getInt(props, "socket.timeout.ms", 30000)  

  val reconnectInterval = Utils.getInt(props, "reconnect.interval", 30000)

  val maxMessageSize = Utils.getInt(props, "max.message.size", 1000000)

  /**
   * This parameter allows you to specify the compression codec for all data generated *
   * by this producer. The default is NoCompressionCodec
   */
  val compressionCodec = Utils.getCompressionCodec(props, "compression.codec")

  /** This parameter allows you to set whether compression should be turned *
   *  on for particular topics
   *
   *  If the compression codec is anything other than NoCompressionCodec,
   *
   *    Enable compression only for specified topics if any
   *
   *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
   *
   *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
   */
  val compressedTopics = Utils.getCSVList(Utils.getString(props, "compressed.topics", null))
}