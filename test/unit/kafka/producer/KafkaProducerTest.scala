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

import kafka.message.{Message, ByteBufferMessageSet}
import junit.framework.{Assert, TestCase}
import kafka.utils.SystemTime
import kafka.TestUtils
import kafka.server.{KafkaServer, KafkaConfig}
import org.apache.log4j.Level

class KafkaProducerTest extends TestCase {
  private var messageBytes =  new Array[Byte](2);
  private var server: KafkaServer = null

  override def setUp() {
    server = TestUtils.createServer(new KafkaConfig(TestUtils.createBrokerConfig(0, 9092))
    {
      override val enableZookeeper = false
    })
  }

  override def tearDown() {
    server.shutdown
  }

  def testUnreachableServer() {
    val producer = new SimpleProducer("NOT_USED", 9092, 100*1024, 300, 1000)
    var failed = false
    val firstStart = SystemTime.milliseconds

    //temporarily increase log4j level to avoid error in output
    producer.setLoggerLevel(Level.FATAL)
    try {
      producer.send("test", 0, new ByteBufferMessageSet(new Message(messageBytes)))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertTrue(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    println("First message send retries took " + (firstEnd-firstStart) + " ms")
    Assert.assertTrue((firstEnd-firstStart) < 300)

    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(new Message(messageBytes)))
    }catch {
      case e: Exception => failed = true

    }
    val secondEnd = SystemTime.milliseconds
    println("Second message send retries took " + (secondEnd-secondStart) + " ms")
    Assert.assertTrue((secondEnd-secondEnd) < 300)
    producer.setLoggerLevel(Level.ERROR)
  }

  def testReachableServer() {
    val producer = new SimpleProducer("localhost", 9092, 100*1024, 500, 1000)
    var failed = false
    val firstStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(new Message(messageBytes)))
    }catch {
      case e: Exception => failed=true
    }
    Assert.assertFalse(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    Assert.assertTrue((firstEnd-firstStart) < 500)
    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(new Message(messageBytes)))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertFalse(failed)
    val secondEnd = SystemTime.milliseconds
    Assert.assertTrue((secondEnd-secondEnd) < 500)
  }

  def testReachableServerWrongPort() {
    val producer = new SimpleProducer("localhost", 9091, 100*1024, 300, 500)
    var failed = false
    val firstStart = SystemTime.milliseconds
    //temporarily increase log4j level to avoid error in output
    producer.setLoggerLevel(Level.FATAL)
    try {
      producer.send("test", 0, new ByteBufferMessageSet(new Message(messageBytes)))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertTrue(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    Assert.assertTrue((firstEnd-firstStart) < 300)
    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(new Message(messageBytes)))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertTrue(failed)
    val secondEnd = SystemTime.milliseconds
    Assert.assertTrue((secondEnd-secondEnd) < 300)
    producer.setLoggerLevel(Level.ERROR)
  }
}