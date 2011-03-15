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

package kafka.javaapi.consumer

import kafka.utils.threadsafe
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.MultiFetchResponse
import kafka.api.FetchRequest
import kafka.javaapi.Implicits._

/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int) {
  val underlying = new kafka.consumer.SimpleConsumer(host, port, soTimeout, bufferSize)

  def fetch(request: FetchRequest): ByteBufferMessageSet = underlying.fetch(request)

  def multifetch(fetches: java.util.List[FetchRequest]): MultiFetchResponse = {
    import scala.collection.JavaConversions._
    underlying.multifetch(asBuffer(fetches): _*)
  }

  /**
   * Get a list of valid offsets (up to maxSize) before the given time.
   * The result is a list of offsets, in descending order.
   * @param time: time in millisecs (if -1, just get from the latest available)
   */
  def getOffsetsBefore(topic: String, partition: Int, time: Long, maxNumOffsets: Int): Array[Long] =
    underlying.getOffsetsBefore(topic, partition, time, maxNumOffsets)

  def close() {
    underlying.close
  }
}