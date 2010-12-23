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

import java.net._
import java.nio._
import java.nio.channels._
import java.util.concurrent.atomic._
import org.apache.log4j.Logger
import kafka.api._
import kafka.common._
import kafka.message._
import kafka.network._
import kafka.utils._

/**
 * A consumer of kafka messages
 */
@threadsafe
class SimpleConsumer(val host: String,
                     val port: Int,
                     val soTimeout: Int,
                     val bufferSize: Int) {
  private val logger = Logger.getLogger(getClass())
  private var channel : SocketChannel = null
  private val lock = new Object()

  private def connect(): SocketChannel = {
    val address = new InetSocketAddress(host, port)

    val channel = SocketChannel.open
    if(logger.isDebugEnabled)
      logger.debug("Connected to " + address + " for fetching.")
    channel.configureBlocking(true)
    channel.socket.setReceiveBufferSize(bufferSize)
    channel.socket.setSoTimeout(soTimeout)
    channel.connect(address)
    if(logger.isTraceEnabled)
      logger.trace("requested receive buffer size=" + bufferSize + " actual receive buffer size= " + channel.socket.getReceiveBufferSize)
    channel
  }

  private def close(channel: SocketChannel) = {
    if(logger.isDebugEnabled)
      logger.debug("Disconnecting from " + channel.socket.getRemoteSocketAddress())
    Utils.swallow(logger.warn, channel.close())
    Utils.swallow(logger.warn, channel.socket.close())
  }

  def close() {
    lock synchronized {
      if (channel != null)
        close(channel)
      channel = null
    }
  }

  /**
   * Fetch a set of messages from the given byte offset, no more than maxSize bytes are fetched.
   */
  def fetch(request: FetchRequest): ByteBufferMessageSet = {
    lock synchronized {
      val startTime = SystemTime.nanoseconds
      getOrMakeConnection()
      var response: Tuple2[Receive,Int] = null
      try {
        sendRequest(request)
        response = getResponse
      } catch {
        case e : java.io.IOException =>
          logger.info("fetch reconnect due to " + e)
          // retry once
          try {
            channel = connect
            sendRequest(request)
            response = getResponse
          }catch {
            case ioe: java.io.IOException => channel = null; throw ioe;
          }
      }
      val endTime = SystemTime.nanoseconds
      SimpleConsumerStats.recordFetchRequest(endTime - startTime)
      new ByteBufferMessageSet(response._1.buffer, response._2)
    }
  }

  def multifetch(fetches: java.util.List[FetchRequest]): MultiFetchResponse = {
    val fetchesArray = fetches.toArray(new Array[FetchRequest](fetches.size))
    multifetch(fetchesArray:_*)
  }

  def multifetch(fetches: FetchRequest*): MultiFetchResponse = {
    lock synchronized {
      val startTime = SystemTime.nanoseconds
      getOrMakeConnection()
      var response: Tuple2[Receive,Int] = null
      try {
        sendRequest(new MultiFetchRequest(fetches.toArray))
        response = getResponse
      } catch {
        case e : java.io.IOException =>
          logger.info("multifetch reconnect due to " + e)
          // retry once
          try {
            channel = connect
            sendRequest(new MultiFetchRequest(fetches.toArray))
            response = getResponse
          }catch {
            case ioe: java.io.IOException => channel = null; throw ioe;
          }
      }
      val endTime = SystemTime.nanoseconds
      SimpleConsumerStats.recordFetchRequest(endTime - startTime)

      // error code will be set on individual messageset inside MultiFetchResponse
      new MultiFetchResponse(response._1.buffer, fetches.length)
    }
  }

  /**
   * Get a list of valid offsets (up to maxSize) before the given time.
   * The result is a list of offsets, in descending order.
   * @param time: time in millisecs (if -1, just get from the latest available)
   */
  def getOffsetsBefore(topic: String, partition: Int, time: Long, maxNumOffsets: Int): Array[Long] = {
    lock synchronized {
      getOrMakeConnection()
      var response: Tuple2[Receive,Int] = null
      try {
        sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets))
        response = getResponse
      } catch {
        case e : java.io.IOException =>
          logger.info("getOffsetsBefore reconnect due to " + e)
          // retry once
          try {
            channel = connect
            sendRequest(new OffsetRequest(topic, partition, time, maxNumOffsets))
            response = getResponse
          }catch {
            case ioe: java.io.IOException => channel = null; throw ioe;
          }
      }
      OffsetRequest.deserializeOffsetArray(response._1.buffer)
    }
  }

  private def sendRequest(request: Request) = {
    val send = new BoundedByteBufferSend(request)
    send.writeCompletely(channel)
  }

  private def getResponse(): Tuple2[Receive,Int] = {
    val response = new BoundedByteBufferReceive()
    response.readCompletely(channel)

    // this has the side effect of setting the initial position of buffer correctly
    val errorCode: Int = response.buffer.getShort
    (response, errorCode)
  }

  private def getOrMakeConnection() {
    if(channel == null) {
      channel = connect()
    }
  }
}

trait SimpleConsumerStatsMBean {
  def getFetchRequestsPerSecond: Double
  def getAvgFetchRequestMs: Double
  def getMaxFetchRequestMs: Double
  def getNumFetchRequests: Long  
}

@threadsafe
class SimpleConsumerStats extends SimpleConsumerStatsMBean {
  private val fetchRequestStats = new SnapshotStats

  def recordFetchRequest(requestNs: Long) = fetchRequestStats.recordRequestMetric(requestNs)

  def getFetchRequestsPerSecond: Double = fetchRequestStats.getRequestsPerSecond

  def getAvgFetchRequestMs: Double = fetchRequestStats.getAvgMetric / (1000.0 * 1000.0)

  def getMaxFetchRequestMs: Double = fetchRequestStats.getMaxMetric / (1000.0 * 1000.0)

  def getNumFetchRequests: Long = fetchRequestStats.getNumRequests
}

object SimpleConsumerStats {
  private val logger = Logger.getLogger(getClass())
  private val simpleConsumerstatsMBeanName = "kafka:type=kafka.SimpleConsumerStats"
  private val stats = new SimpleConsumerStats
  Utils.swallow(logger.warn, Utils.registerMBean(stats, simpleConsumerstatsMBeanName))

  def recordFetchRequest(requestMs: Long) = stats.recordFetchRequest(requestMs)
}
