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

package kafka.log

import junit.framework.TestCase
import java.io.File
import kafka.TestUtils
import kafka.utils.Utils
import kafka.message.{ByteBufferMessageSet, Message}
import kafka.server.{KafkaConfig, KafkaServer}
import junit.framework.Assert._
import java.util.{Random, Properties}
import kafka.api.{FetchRequest, OffsetRequest}
import collection.mutable.WrappedArray
import kafka.consumer.SimpleConsumer

object LogOffsetTest {
  val random = new Random()  
}

class LogOffsetTest extends TestCase {
  var logDir: File = null
  var topicLogDir: File = null
  var server: KafkaServer = null
  var logSize: Int = 100
  val brokerPort: Int = 9099
  var simpleConsumer: SimpleConsumer = null

  override def setUp() {
    val config: Properties = createBrokerConfig(1, brokerPort)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)
    
    server = TestUtils.createServer(new KafkaConfig(config))
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024)
  }

  override def tearDown() {
    simpleConsumer.close
    server.shutdown
    Utils.rm(logDir)
  }

  def testEmptyLogs() {
    val messageSet: ByteBufferMessageSet = simpleConsumer.fetch(
      new FetchRequest("test", 0, 0, 300 * 1024))
    assertFalse(messageSet.iterator.hasNext)

    {
      val offsets = simpleConsumer.getOffsetsBefore("test", 0, OffsetRequest.LATEST_TIME, 10)
      assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )
    }

    {
      val offsets = simpleConsumer.getOffsetsBefore("test", 0, OffsetRequest.EARLIEST_TIME, 10)
      assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )
    }

    {
      val offsets = simpleConsumer.getOffsetsBefore("test", 0, 1295978400000L, 10)
      assertTrue( 0 == offsets.length )
    }

  }

  def testGetOffsetsBeforeLatestTime() {
    val topicPartition = "kafka-" + 0
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)

    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(message))
    log.flush()

    Thread.sleep(100)

    val offsetRequest = new OffsetRequest(topic, part,
                                          OffsetRequest.LATEST_TIME, 10)

    val offsets = log.getOffsetsBefore(offsetRequest)

    assertTrue((Array(220L, 110L, 0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]))

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.LATEST_TIME, 10)
    assertTrue((Array(220L, 110L, 0L): WrappedArray[Long]) == (consumerOffsets: WrappedArray[Long]))

    // try to fetch using latest offset
    val messageSet: ByteBufferMessageSet = simpleConsumer.fetch(
      new FetchRequest(topic, 0, consumerOffsets.head, 300 * 1024))
    assertFalse(messageSet.iterator.hasNext)
  }

  def testGetOffsetsBeforeNow() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(message))
    log.flush()

    Thread.sleep(100)

    val offsetRequest = new OffsetRequest(topic, part,
                                           System.currentTimeMillis, 10)
    val offsets = log.getOffsetsBefore(offsetRequest)
    assertTrue((Array(220L, 110L, 0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]))

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          System.currentTimeMillis, 10)
    assertTrue((Array(220L, 110L, 0L): WrappedArray[Long]) == (consumerOffsets: WrappedArray[Long]))
  }

  def testGetOffsetsBeforeEarliestTime() {
    val topicPartition = "kafka-" + LogOffsetTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val logManager = server.getLogManager
    val log = logManager.getOrCreateLog(topic, part)
    val message = new Message(Integer.toString(42).getBytes())
    for(i <- 0 until 20)
      log.append(new ByteBufferMessageSet(message))
    log.flush()

    Thread.sleep(100)

    val offsetRequest = new OffsetRequest(topic, part,
                                          OffsetRequest.EARLIEST_TIME, 10)
    val offsets = log.getOffsetsBefore(offsetRequest)

    assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.EARLIEST_TIME, 10)
    assertTrue( (Array(0L): WrappedArray[Long]) == (offsets: WrappedArray[Long]) )
  }

  private def createBrokerConfig(nodeId: Int, port: Int): Properties = {
    val props = new Properties
    props.put("brokerid", nodeId.toString)
    props.put("port", port.toString)
    props.put("log.dir", getLogDir.getAbsolutePath)
    props.put("log.flush.interval", "1")
    props.put("enable.zookeeper", "false")
    props.put("num.partitions", "20")
    props.put("log.retention.hours", "10")
    props.put("log.cleanup.interval.mins", "5")
    props.put("log.file.size", logSize.toString)
    props
  }

  private def getLogDir(): File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val f = new File(ioDir, "kafka-logs")
    f.mkdirs()
    f.deleteOnExit()
    f
  }

}
