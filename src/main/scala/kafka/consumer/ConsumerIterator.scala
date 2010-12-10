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

import kafka.utils.IteratorTemplate
import org.apache.log4j.Logger
import java.util.concurrent.{TimeUnit, BlockingQueue}
import kafka.message.{MessageSet, Message}

/**
 * An iterator that blocks until a value can be read from the supplied queue.
 * The iterator takes a shutdownCommand object which can be added to the queue to trigger a shutdown
 * 
 */
class ConsumerIterator(private val channel: BlockingQueue[FetchedDataChunk], consumerTimeoutMs: Int)
        extends IteratorTemplate[Message] {
  
  private val logger = Logger.getLogger(classOf[ConsumerIterator])
  private var current: Iterator[Message] = null
  private var currentTopicInfo: PartitionTopicInfo = null

  override def next(): Message = {
    val message = super.next
    currentTopicInfo.consumed(MessageSet.entrySize(message))
    message
  }

  protected def makeNext(): Message = {
    // if we don't have an iterator, get one
    if(current == null || !current.hasNext) {
      var found: FetchedDataChunk = null
      if (consumerTimeoutMs < 0)
        found = channel.take
      else {
        found = channel.poll(consumerTimeoutMs, TimeUnit.MILLISECONDS)
        if (found == null) {
          logger.debug("Consumer iterator timing out..")
          throw new ConsumerTimeoutException
        }
      }
      if(found eq ZookeeperConsumerConnector.shutdownCommand) {
        logger.debug("Received the shutdown command")
    	  channel.offer(found)
        return allDone
      } else {
        currentTopicInfo = found.topicInfo
        current = found.messages.iterator
      }
    }
    current.next
  }
  
}

class ConsumerTimeoutException() extends RuntimeException()
