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

import async.{AsyncKafkaProducer, ProducerConfig, QueueClosedException, QueueFullException}
import kafka.message.{ByteBufferMessageSet, Message}
import junit.framework.{Assert, TestCase}
import java.util.Properties
import org.easymock.EasyMock
import kafka.api.ProducerRequest
import kafka.serializer.SerDeser
import org.apache.log4j.Level

class AsyncProducerTest extends TestCase {

  private val messageContent1 = "test"
  private val topic1 = "test-topic"
  private val message1: Message = new Message(messageContent1.getBytes)

  private val messageContent2 = "test1"
  private val topic2 = "test1-topic"
  private val message2: Message = new Message(messageContent2.getBytes)

  def testProducerQueueSize() {
    val basicProducer = EasyMock.createMock(classOf[SimpleProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 10)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)
    
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    val config = new ProducerConfig(props)
    
    val producer = new AsyncKafkaProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    //temporarily set log4j to a higher level to avoid error in the output
    producer.setLoggerLevel(Level.FATAL)
    
    try {
      for(i <- 0 until 11) {
        producer.send(messageContent1)
      }
      Assert.fail("Queue should be full")
    }
    catch {
      case e: QueueFullException =>
    }
    producer.close
    EasyMock.verify(basicProducer)
    producer.setLoggerLevel(Level.ERROR)    
  }

  def testAddAfterQueueClosed() {
    val basicProducer = EasyMock.createMock(classOf[SimpleProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 10)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    val config = new ProducerConfig(props)

    val producer = new AsyncKafkaProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    for(i <- 0 until 10) {
      producer.send(messageContent1)
    }
    producer.close

    try {
      producer.send(messageContent1)
      Assert.fail("Queue should be closed")
    } catch {
      case e: QueueClosedException =>
    }
    EasyMock.verify(basicProducer)
  }
  
  def testBatchSize() {
    val basicProducer = EasyMock.createStrictMock(classOf[SimpleProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 5)))))
    EasyMock.expectLastCall.times(2)
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 1)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("batch.size", "5")

    val config = new ProducerConfig(props)

    val producer = new AsyncKafkaProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    for(i <- 0 until 10) {
      producer.send(messageContent1)
    }

    Thread.sleep(100)
    try {
      producer.send(messageContent1)
    } catch {
      case e: QueueFullException =>
        Assert.fail("Queue should not be full")
    }

    producer.close
    EasyMock.verify(basicProducer)
  }

  def testQueueTimeExpired() {
    val basicProducer = EasyMock.createMock(classOf[SimpleProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic1, ProducerRequest.RandomPartition,
      getMessageSetOfSize(List(message1), 3)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("queue.time", "200")

    val config = new ProducerConfig(props)

    val producer = new AsyncKafkaProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    for(i <- 0 until 3) {
      producer.send(messageContent1)
    }

    Thread.sleep(500)
    producer.close
    EasyMock.verify(basicProducer)
  }

  def testSenderThreadShutdown() {
    val basicProducer = new MockProducer("localhost", 9092, 1000, 1000, 1000)
    
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "10")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("queue.time", "100")

    val config = new ProducerConfig(props)
    val producer = new AsyncKafkaProducer[String](config, basicProducer, new StringSerializer)
    producer.start
    producer.send(messageContent1)
    producer.close
  }

  def testCollateEvents() {
    val basicProducer = EasyMock.createMock(classOf[SimpleProducer])
    basicProducer.multiSend(EasyMock.aryEq(Array(new ProducerRequest(topic2, ProducerRequest.RandomPartition,
                                                                     getMessageSetOfSize(List(message2), 5)),
                                                 new ProducerRequest(topic1, ProducerRequest.RandomPartition,
                                                                     getMessageSetOfSize(List(message1), 5)))))
    EasyMock.expectLastCall
    basicProducer.close
    EasyMock.expectLastCall
    EasyMock.replay(basicProducer)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", "9092")
    props.put("queue.size", "50")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("batch.size", "10")

    val config = new ProducerConfig(props)

    val producer = new AsyncKafkaProducer[String](config, basicProducer, new StringSerializer)

    producer.start
    for(i <- 0 until 5) {
      producer.send(messageContent1)
      producer.send(messageContent2)
    }

    producer.close
    EasyMock.verify(basicProducer)
    
  }
  
  private def getMessageSetOfSize(messages: List[Message], counts: Int): ByteBufferMessageSet = {
    var messageList = new java.util.ArrayList[Message]()
    for(message <- messages) {
      for(i <- 0 until counts) {
        messageList.add(message)
      }
    }
    new ByteBufferMessageSet(messageList)
  }

  class StringSerializer extends SerDeser[String] {
    def toEvent(message: Message):String = message.toString
    def toMessage(event: String):Message = new Message(event.getBytes)
    def getTopic(event: String): String = event.concat("-topic")
  }

  class MockProducer(override val host: String,
                     override val port: Int,
                     override val bufferSize: Int,
                     override val connectTimeoutMs: Int,
                     override val reconnectInterval: Int) extends
  SimpleProducer(host, port, bufferSize, connectTimeoutMs, reconnectInterval) {
    override def send(topic: String, messages: ByteBufferMessageSet): Unit = {
      Thread.sleep(1000)
    }
    override def multiSend(produces: Array[ProducerRequest]) {
      Thread.sleep(1000)
    }
  }
}