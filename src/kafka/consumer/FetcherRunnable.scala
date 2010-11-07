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

import java.util.concurrent.CountDownLatch
import org.apache.log4j.Logger
import java.nio.channels.{ClosedChannelException, ClosedByInterruptException}
import kafka.common.{OffsetOutOfRangeException, ErrorMapping}
import kafka.cluster.{Partition, Broker}
import kafka.api.{MultiFetchResponse, OffsetRequest, FetchRequest}
import kafka.utils.{StringSerializer, ZkUtils, ZKGroupTopicDirs, Utils}
import org.I0Itec.zkclient.ZkClient

class FetcherRunnable(val name: String,
                      val zkClient : ZkClient,
                      val config: ConsumerConfig,
                      val broker: Broker,
                      val partitionTopicInfos: List[PartitionTopicInfo]) 
        extends Thread(name) {
  private val logger = Logger.getLogger(getClass())
  private val shutdownLatch = new CountDownLatch(1)
  private val simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs,
    config.socketBufferSize)
  @volatile
  private var stopped = false

  def shutdown(): Unit = {
    stopped = true
    interrupt
    logger.debug("awaiting shutdown on fetcher " + name)
    shutdownLatch.await
    logger.debug("shutdown of fetcher " + name + " thread complete")
  }

  override def run() {
    for (info <- partitionTopicInfos)
      logger.info(name + " start fetching topic: " + info.topic + " part: " + info.partition.partId + " offset: "
        + info.fetchedOffset.get + " from " + broker.host + ":" + broker.port)

    try {
      while (!stopped) {
        val fetches = partitionTopicInfos.map(info =>
               new FetchRequest(info.topic, info.partition.partId, info.fetchedOffset.get, config.fetchSize))

        if (logger.isTraceEnabled)
          logger.trace("fetch request: " + fetches.toString)

        val response = simpleConsumer.multifetch(fetches : _*)

        var read = 0

        for((messages, info) <- response.zip(partitionTopicInfos.iterator)) {
          var done = false
          if(messages.errorCOde == ErrorMapping.OFFSET_OUT_OF_RANGE_CODE) {
            logger.info("offset " + info.fetchedOffset.get + " out of range")
            // see if we can fix this error
            if (info.fetchedOffset.get == info.consumedOffset.get) {
              val resetOffset = resetConsumerOffsets(info.topic, info.partition)
              if(resetOffset >= 0) {
                info.fetchedOffset.set(resetOffset)
                info.consumedOffset.set(resetOffset)
                done = true
              }
            }
          }
          if (!done) {
            if(messages.sizeInBytes > 0 || messages.errorCOde != ErrorMapping.NO_ERROR)
              read += info.enqueue(messages)
          }
        }
        if (logger.isTraceEnabled)
          logger.trace("fetched bytes: " + read)
        if(read == 0)
          Thread.sleep(config.backoffIncrementMs)
      }
    }
    catch {
      case e =>
        if (stopped)
          logger.info("FecherRunnable " + this + " interrupted")
        else
          logger.error("error in FetcherRunnable " + e + Utils.stackTrace(e))
    }

    logger.info("stopping fetcher " + name + " to host " + broker.host)
    Utils.swallow(logger.info, simpleConsumer.close)
    shutdownComplete()
  }

  /**
   * Record that the thread shutdown is complete
   */
  private def shutdownComplete() = shutdownLatch.countDown

  private def resetConsumerOffsets(topic : String,
                                   partition: Partition) : Long = {
    var offset : Long = 0
    config.autoOffsetReset match {
      case OffsetRequest.SMALLEST_TIME_STRING => offset = OffsetRequest.EARLIEST_TIME
      case OffsetRequest.LARGEST_TIME_STRING => offset = OffsetRequest.LATEST_TIME
      case _ => return -1
    }

    // get mentioned offset from the broker
    val offsets = simpleConsumer.getOffsetsBefore(topic, partition.partId, offset, 1)
    val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
    
    // reset manually in zookeeper
    logger.info("updating partition " + partition.name + " with " + (if(offset == OffsetRequest.EARLIEST_TIME) "earliest " else " latest ") + "offset " + offsets(0))
    ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition.name, offsets(0).toString)

    offsets(0)
  }
}
