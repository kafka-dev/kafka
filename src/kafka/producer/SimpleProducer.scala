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

import java.net._
import java.nio.channels._
import kafka.message._
import kafka.network._
import kafka.utils._
import kafka.api._
import scala.math._
import org.apache.log4j.{Level, Logger}

object SimpleProducer {
  val RequestKey: Short = 0
}

/*
 * Send a message set.
 */
@threadsafe
class SimpleProducer(val host: String, 
                     val port: Int,
                     val bufferSize: Int,
                     val connectTimeoutMs: Int,
                     val reconnectInterval: Int) {
  
  private val logger = Logger.getLogger(getClass())
  private val MaxConnectBackoffMs = 60000
  private var channel : SocketChannel = null
  private var sentOnConnection = 0
  private val lock = new Object()
  @volatile
  private var shutdown: Boolean = false

  /**
   * Common functionality for the public send methods
   */
  private def send(send: BoundedByteBufferSend) {
    lock synchronized {
      val startTime = SystemTime.nanoseconds
      getOrMakeConnection()

      try {
        send.writeCompletely(channel)
      } catch {
        case e : java.io.IOException =>
          // no way to tell if write succeeded. Disconnect and re-throw exception to let client handle retry
          disconnect()
          throw e
      }
      // TODO: do we still need this?
      sentOnConnection += 1
      if(sentOnConnection >= reconnectInterval) {
        disconnect()
        channel = connect()
        sentOnConnection = 0
      }
      val endTime = SystemTime.nanoseconds
      KafkaProducerStats.recordProduceRequest(endTime - startTime)
    }
  }

  /**
   * Send a message
   */
  def send(topic: String, partition: Int, messages: ByteBufferMessageSet) {
    val setSize = messages.sizeInBytes.asInstanceOf[Int]
    if(logger.isTraceEnabled)
      logger.trace("Got message set with " + setSize + " bytes to send")
    send(new BoundedByteBufferSend(new ProducerRequest(topic, partition, messages)))
  }
 
  def send(topic: String, messages: ByteBufferMessageSet): Unit = send(topic, ProducerRequest.RandomPartition, messages)

  def multiSend(produces: Array[ProducerRequest]) {
    val setSize = produces.foldLeft(0L)(_ + _.messages.sizeInBytes)
    if(logger.isTraceEnabled)
      logger.trace("Got multi message sets with " + setSize + " bytes to send")
    send(new BoundedByteBufferSend(new MultiProducerRequest(produces)))
  }

  def close() = {
    lock synchronized {
      disconnect()
      shutdown = true
    }
  }

  /**
   * Disconnect from current channel, closing connection.
   * Side effect: channel field is set to null on successful disconnect
   */
  private def disconnect() {
    try {
      if(channel != null) {
        logger.debug("Disconnecting from " + host + ":" + port)
        Utils.swallow(logger.warn, channel.close())
        Utils.swallow(logger.warn, channel.socket.close())
        channel = null
      }
    } catch {
      case e: Exception => logger.error("Error on disconnect: ", e)
    }
  }
    
  private def connect(): SocketChannel = {
    var channel: SocketChannel = null
    var connectBackoffMs = 1
    val beginTimeMs = SystemTime.milliseconds
    while(channel == null && !shutdown) {
      try {
        logger.debug("Connecting to " + host + ":" + port + " for producing")
        channel = SocketChannel.open()
        channel.socket.setSendBufferSize(bufferSize)
        channel.connect(new InetSocketAddress(host, port))
        channel.configureBlocking(true)
      }
      catch {
        case e: Exception => {
          disconnect()
          val endTimeMs = SystemTime.milliseconds
          if ( (endTimeMs - beginTimeMs + connectBackoffMs) > connectTimeoutMs)
          {
            logger.error("Producer connection timing out after " + connectTimeoutMs + " ms")
            throw e
          }
          logger.error("Connection attempt failed, next attempt in " + connectBackoffMs + " ms", e)
          SystemTime.sleep(connectBackoffMs)
          connectBackoffMs = min(10 * connectBackoffMs, MaxConnectBackoffMs)
        }
      }
    }
    channel
  }

  private def getOrMakeConnection() {
    if(channel == null) {
      channel = connect()
    }
  }
}

trait KafkaProducerStatsMBean {
  def getProduceRequestsPerSecond: Double
  def getAvgProduceRequestMs: Double
  def getMaxProduceRequestMs: Double
  def getNumProduceRequests: Long
}

@threadsafe
class KafkaProducerStats extends KafkaProducerStatsMBean {
  private val produceRequestStats = new SnapshotStats

  def recordProduceRequest(requestNs: Long) = produceRequestStats.recordRequestMetric(requestNs)

  def getProduceRequestsPerSecond: Double = produceRequestStats.getRequestsPerSecond

  def getAvgProduceRequestMs: Double = produceRequestStats.getAvgMetric / (1000.0 * 1000.0)

  def getMaxProduceRequestMs: Double = produceRequestStats.getMaxMetric / (1000.0 * 1000.0)

  def getNumProduceRequests: Long = produceRequestStats.getNumRequests
}

object KafkaProducerStats {
  private val logger = Logger.getLogger(getClass())
  private val kafkaProducerstatsMBeanName = "kafka:type=kafka.KafkaProducerStats"
  private val stats = new KafkaProducerStats
  Utils.swallow(logger.warn, Utils.registerMBean(stats, kafkaProducerstatsMBeanName))

  def recordProduceRequest(requestMs: Long) = stats.recordProduceRequest(requestMs)
}
