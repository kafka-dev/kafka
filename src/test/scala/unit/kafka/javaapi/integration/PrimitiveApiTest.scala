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

package kafka.javaapi.integration

import scala.collection._
import junit.framework.TestCase
import junit.framework.Assert._
import kafka.TestUtils
import kafka.api.{FetchRequest}
import kafka.message.{Message}
import kafka.common.{InvalidPartitionException, OffsetOutOfRangeException}
import kafka.server.{KafkaRequestHandlers, KafkaConfig}
import org.apache.log4j.{Level, Logger}
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.ProducerRequest
import kafka.javaapi.MultiFetchResponse

/**
 * End to end tests of the primitive apis against a local server
 */
class PrimitiveApiTest extends JUnitSuite with ProducerConsumerTestHarness with KafkaServerTestHarness {
  
  val port = 9999
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
                 override val enableZookeeper = false
               }
  val configs = List(config)
  val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  @Test
  def testProduceAndFetch() {
    // send some messages
    val topic = "test"

//    send an empty messageset first
    val sent2 = new ByteBufferMessageSet(getMessageList(Seq.empty[Message]: _*))
    producer.send(topic, sent2)
    Thread.sleep(200)
    sent2.buffer.rewind
    var fetched2 = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    TestUtils.checkEquals(sent2.iterator, fetched2.iterator)

    // send some messages
    val sent3 = new ByteBufferMessageSet(getMessageList(new Message("hello".getBytes()),
      new Message("there".getBytes())))
    producer.send(topic, sent3)
    Thread.sleep(200)
    sent3.buffer.rewind
    var fetched3: ByteBufferMessageSet = null
    while(fetched3 == null || fetched3.validBytes == 0)
      fetched3 = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    TestUtils.checkEquals(sent3.iterator, fetched3.iterator)

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    // send an invalid offset
    try {
      val fetchedWithError = consumer.fetch(new FetchRequest(topic, 0, -1, 10000))
      fetchedWithError.iterator
      fail("expect exception")
    }
    catch {
      case e: OffsetOutOfRangeException => "this is good"
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  @Test
  def testProduceAndMultiFetch() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics) {
        val set = new ByteBufferMessageSet(getMessageList(new Message(("a_" + topic).getBytes),
          new Message(("b_" + topic).getBytes)))
        messages += topic -> set
        producer.send(topic, set)
        set.buffer.rewind
        fetches += new FetchRequest(topic, 0, 0, 10000)
      }

      // wait a bit for produced message to be available
      Thread.sleep(200)
      val response = consumer.multifetch(getFetchRequestList(fetches: _*))
      for((topic, resp) <- topics.zip(response.toList))
    	  TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
    }

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    {
      // send some invalid offsets
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, 0, -1, 10000)

      try {
        val responses = consumer.multifetch(getFetchRequestList(fetches: _*))
        for(resp <- responses)
    	    resp.iterator
        fail("expect exception")
      }
      catch {
        case e: OffsetOutOfRangeException => "this is good"
      }
    }    

    {
      // send some invalid partitions
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, -1, 0, 10000)

      try {
        val responses = consumer.multifetch(getFetchRequestList(fetches: _*))
        for(resp <- responses)
    	    resp.iterator
        fail("expect exception")
      }
      catch {
        case e: InvalidPartitionException => "this is good"
      }
    }

    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
  }

  @Test
  def testProduceAndMultiFetchJava() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val fetches : java.util.ArrayList[FetchRequest] = new java.util.ArrayList[FetchRequest]
      for(topic <- topics) {
        val set = new ByteBufferMessageSet(getMessageList(new Message(("a_" + topic).getBytes),
          new Message(("b_" + topic).getBytes)))
        messages += topic -> set
        producer.send(topic, set)
        set.buffer.rewind
        fetches.add(new FetchRequest(topic, 0, 0, 10000))
      }

      // wait a bit for produced message to be available
      Thread.sleep(200)
      val response = consumer.multifetch(fetches)
      for((topic, resp) <- topics.zip(response.toList))
    	  TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
    }
  }

  @Test
  def testMultiProduce() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val fetches = new mutable.ArrayBuffer[FetchRequest]
    var produceList: List[ProducerRequest] = Nil
    for(topic <- topics) {
      val set = new ByteBufferMessageSet(getMessageList(new Message(("a_" + topic).getBytes),
        new Message(("b_" + topic).getBytes)))
      messages += topic -> set
      produceList ::= new ProducerRequest(topic, 0, set)
      fetches += new FetchRequest(topic, 0, 0, 10000)
    }
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.buffer.rewind
      
    // wait a bit for produced message to be available
    Thread.sleep(200)
    val response = consumer.multifetch(getFetchRequestList(fetches: _*))
    for((topic, resp) <- topics.zip(response.toList))
  	  TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
  }

  private def getMessageList(messages: Message*): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messages.foreach(m => messageList.add(m))
    messageList
  }

  private def getFetchRequestList(fetches: FetchRequest*): java.util.List[FetchRequest] = {
    val fetchReqs = new java.util.ArrayList[FetchRequest]()
    fetches.foreach(f => fetchReqs.add(f))
    fetchReqs
  }
}
