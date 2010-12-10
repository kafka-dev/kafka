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

import kafka.utils.SystemTime
import java.util.concurrent.{TimeUnit, CountDownLatch, BlockingQueue}
import org.apache.log4j.Logger
import collection.mutable.ListBuffer
import kafka.serializer.SerDeser

class ProducerSendThread[T](val queue: BlockingQueue[T],
                            val serializer: SerDeser[T],
                            val handler: EventHandler[T],
                            val queueTime: Long,
                            val batchSize: Int,
                            val shutdownCommand: Any) extends Thread {

  private val logger = Logger.getLogger(classOf[ProducerSendThread[T]])
  private var running: Boolean = true
  private val shutdownLatch = new CountDownLatch(1) 
  
  override def run {

    try {
      val remainingEvents = processEvents
      if(logger.isDebugEnabled) logger.debug("Remaining events = " + remainingEvents.size)
      
      // handle remaining events
      if(remainingEvents.size > 0)
        tryToHandle(remainingEvents)
    }catch {
      case e: Exception => logger.error("Error in sending events")
    }finally {
      shutdownLatch.countDown
    }
  }

  def awaitShutdown = shutdownLatch.await
  
  def shutdown = {
    running = false
    handler.close
    if(logger.isDebugEnabled)
      logger.debug("Shutdown thread complete")
  }

  private def processEvents(): Seq[T] = {
    var now = SystemTime.milliseconds
    var lastSend = now

    var events = new ListBuffer[T]
    while(running) {
      val current: T = queue.poll(scala.math.max(0, queueTime - (lastSend - now)), TimeUnit.MILLISECONDS)
      if(current == shutdownCommand) {
        return events
      }

      if(current != null)
        events += current

      now = SystemTime.milliseconds

      // time to send messages
      val expired: Boolean = (now - lastSend) > queueTime
      val full: Boolean = events.size >= batchSize
      if(expired || full) {
        if(logger.isDebugEnabled && full) logger.debug("Batch full. Sending..")
        if(logger.isDebugEnabled && expired) logger.debug("Queue time reached. Sending..")
        tryToHandle(events)
        lastSend = now
        events = new ListBuffer[T]
      }
    }
    events
  }
  
  def tryToHandle(events: Seq[T]) {
    try {
      if(logger.isDebugEnabled) logger.debug("Handling " + events.size + " events")
      handler.handle(events)
    }catch {
      case e: Exception => logger.error("Error in handling batch of " + events.size + " events")
    }
  }
}