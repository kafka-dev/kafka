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

import junit.framework.TestCase
import java.io.File
import kafka.TestUtils
import kafka.utils.Utils
import kafka.message.{ByteBufferMessageSet, Message}
import kafka.log.Log
import kafka.server.{KafkaConfig, KafkaServer}
import junit.framework.Assert._
import java.util.{Random, Properties}
import kafka.api.{FetchRequest, OffsetRequest}

object SimpleConsumerTest {
  val random = new Random()  
}

class SimpleConsumerTest extends TestCase {
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

  def testSimpleConsumerEmptyLogs() {
    val offsets = simpleConsumer.getOffsetsBefore("test", 0, OffsetRequest.LATEST_TIME, 10)

    assertEquals(0, offsets.length)
  }

  def testEmptyLogs() {
    val topicPartition = "kafka-" + SimpleConsumerTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir

    val log = new Log(topicLogDir, logSize, 1000)
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    val offsetRequest = new OffsetRequest(topic, part,
                                          OffsetRequest.LATEST_TIME, 10)
    val offsets = log.getOffsetsBefore(offsetRequest)

    assertEquals(0, offsets.length)

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.LATEST_TIME, 10)
    assertEquals(0, consumerOffsets.length)

    val messageSet: ByteBufferMessageSet = simpleConsumer.fetch(
      new FetchRequest(topic, 0, 0, 300 * 1024))
    assertFalse(messageSet.iterator.hasNext)
  }
  
  def testEmptyLogsGetOffsets() {
    val topicPartition = "kafka-" + SimpleConsumerTest.random.nextInt(10)
    val topicPartitionPath = getLogDir.getAbsolutePath + "/" + topicPartition
    topicLogDir = new File(topicPartitionPath)
    topicLogDir.mkdir
    
    val topic = topicPartition.split("-").head
    val part = Integer.valueOf(topicPartition.split("-").last).intValue

    var offsetChanged = false
    for(i <- 1 to 14) {
      val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
        OffsetRequest.EARLIEST_TIME, 1)

      if(consumerOffsets(0) == 1) {
        offsetChanged = true
      }
    }
    assertFalse(offsetChanged)
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

    assertEquals(220L, offsets.head)

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.LATEST_TIME, 10)
    assertEquals(220L, consumerOffsets.head)

    // try to fetch using latest offset
    val messageSet: ByteBufferMessageSet = simpleConsumer.fetch(
      new FetchRequest(topic, 0, consumerOffsets.head, 300 * 1024))
    assertFalse(messageSet.iterator.hasNext)
  }

  def testGetOffsetsBeforeNow() {
    val topicPartition = "kafka-" + SimpleConsumerTest.random.nextInt(10)
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

    assertEquals(220L, offsets.head)

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          System.currentTimeMillis, 10)
    assertEquals(220L, consumerOffsets.head)
  }

  def testGetOffsetsBeforeEarliestTime() {
    val topicPartition = "kafka-" + SimpleConsumerTest.random.nextInt(10)
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

    assertEquals(0L, offsets.head)

    val consumerOffsets = simpleConsumer.getOffsetsBefore(topic, part,
                                                          OffsetRequest.EARLIEST_TIME, 10)
    assertEquals(0L, consumerOffsets.head)
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
