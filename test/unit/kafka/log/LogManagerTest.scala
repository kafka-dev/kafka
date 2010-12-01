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

import java.io._
import kafka.utils._
import kafka.message._
import kafka.common._
import junit.framework.TestCase
import junit.framework.Assert._
import kafka.TestUtils
import kafka.server.KafkaConfig

class LogManagerTest extends TestCase {

  val time: MockTime = new MockTime()
  val maxLogAge = 1000
  var logDir: File = null
  var logManager: LogManager = null
  var config:KafkaConfig = null
  override def setUp() {
    val props = TestUtils.createBrokerConfig(0, -1)
    config = new KafkaConfig(props) {
                   override val logFileSize = 1024
                   override val enableZookeeper = false
                 }
    logManager = new LogManager(config, null, time, -1, maxLogAge)
    logManager.startup
    logDir = logManager.logDir
  }
  
  override def tearDown() {
    logManager.close()
    Utils.rm(logDir)
  }
  
  def testCreateLog() {
    val log = logManager.getOrCreateLog("kafka", 0)
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }


  def testCleanup() {
    val log = logManager.getOrCreateLog("cleanup", 0)
    var offset = 0L
    for(i <- 0 until 1000) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
      offset += set.sizeInBytes
    }
    assertTrue("There should be more than one segment now.", log.numberOfSegments > 1)
    time.currentMs += maxLogAge + 10000
    logManager.cleanupLogs()
    assertEquals("Now there should only be only one segment.", 1, log.numberOfSegments)
    assertEquals("Should get empty fetch off new log.", 0L, log.read(offset, 1024).sizeInBytes)
    try {
      log.read(0, 1024)
      fail("Should get exception from fetching earlier.")
    } catch {
      case e: OffsetOutOfRangeException => "This is good."
    }
    // log should still be appendable
    log.append(TestUtils.singleMessageSet("test".getBytes()))
  }

  def testTimeBasedFlush() {
    val props = TestUtils.createBrokerConfig(0, -1)
    logManager.close
    Thread.sleep(100)
    config = new KafkaConfig(props) {
                   override val logFileSize = 1024 *1024 *1024 
                   override val enableZookeeper = false
                   override val flushSchedulerThreadRate = 50
                   override val flushInterval = Int.MaxValue
                   override val flushIntervalMap = Utils.getTopicFlushIntervals("timebasedflush:100")
                 }
    logManager = new LogManager(config, null, time, -1, maxLogAge)
    logManager.startup
    val log = logManager.getOrCreateLog("timebasedflush", 0)
    for(i <- 0 until 200) {
      var set = TestUtils.singleMessageSet("test".getBytes())
      log.append(set)
    }

    assertTrue("The last flush time has to be within defaultflushInterval of current time ",
                     (System.currentTimeMillis - log.getLastFlushedTime) < 100)
  }

  def testConfigurablePartitions() {
    val props = TestUtils.createBrokerConfig(0, -1)
    logManager.close
    Thread.sleep(100)
    config = new KafkaConfig(props) {
                   override val logFileSize = 256
                   override val enableZookeeper = false
                   override val topicPartitionsMap = Utils.getTopicPartitions("testPartition:2")
                 }
    
    logManager = new LogManager(config, null, time, -1, maxLogAge)
    logManager.startup
    
    for(i <- 0 until 2) {
      val log = logManager.getOrCreateLog("testPartition", i)
      for(i <- 0 until 250) {
        var set = TestUtils.singleMessageSet("test".getBytes())
        log.append(set)
      }
    }

    try
    {
      val log = logManager.getOrCreateLog("testPartition", 2)
      assertTrue("Should not come here", log != null)
    } catch {
       case _ =>
    }
  }
}
