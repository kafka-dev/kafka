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

import java.nio.channels._
import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.message._
import kafka.cluster._

class PartitionTopicInfo(val topic: String, 
                         val brokerId: Int,
                         val partition: Partition,
                         val chunkQueue: BlockingQueue[FetchedDataChunk],
                         val consumedOffset: AtomicLong,
                         val fetchedOffset: AtomicLong,
                         val fetchSize: AtomicInteger) {
  
  /**
   * Record the given number of bytes as having been consumed
   */
  def consumed(messageSize: Int): Unit = 
    consumedOffset.addAndGet(messageSize)
  
  /**
   * Enqueue a message set for processing
   * @return the number of valid bytes
   */
  def enqueue(messages: ByteBufferMessageSet): Int = {
    val size = messages.validBytes
    if(size > 0) {
      fetchedOffset.addAndGet(size)
      chunkQueue.put(new FetchedDataChunk(messages, this))
    }
    size
  }

  override def toString(): String = topic + ":" + partition.toString
}
