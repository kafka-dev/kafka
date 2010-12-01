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

package kafka.integration

import scala.collection._
import junit.framework.TestCase
import junit.framework.Assert._
import kafka.TestUtils
import kafka.api.{ProducerRequest, FetchRequest}
import kafka.message.{Message, ByteBufferMessageSet}
import kafka.server.KafkaConfig
import kafka.common.{WrongPartitionException, OffsetOutOfRangeException}


/**
 * End to end tests of the primitive apis against a local server
 */
class PrimitiveApiTest extends TestCase with ProducerConsumerTestHarness with KafkaServerTestHarness {
  
  val port = 9999
  val props = TestUtils.createBrokerConfig(0, port)
  val config = new KafkaConfig(props) {
                 override val enableZookeeper = false
               }
  val configs = List(config)
  
  def testProduceAndFetch() {
    // send some messages
    val topic = "test"
    // send an empty messageset first
    val sent2 = new ByteBufferMessageSet(new java.util.ArrayList[Message]())
    producer.send(topic, sent2)
    Thread.sleep(200)
    sent2.buffer.rewind
    var fetched2 = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    TestUtils.checkEquals(sent2.iterator, fetched2.iterator)

    // send some messages
    val sent3 = new ByteBufferMessageSet(new Message("hello".getBytes()), new Message("there".getBytes()))
    producer.send(topic, sent3)
    sent3.buffer.rewind
    var fetched3: ByteBufferMessageSet = null
    while(fetched3 == null || fetched3.validBytes == 0)
      fetched3 = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
    TestUtils.checkEquals(sent3.iterator, fetched3.iterator)

    // send an invalid offset
    try {
      val fetchedWithError = consumer.fetch(new FetchRequest(topic, 0, -1, 10000))
      fetchedWithError.iterator
      fail("expect exception")
    }
    catch {
      case e: OffsetOutOfRangeException => "this is good"
    }
  }

  def testProduceAndMultiFetch() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics) {
        val set = new ByteBufferMessageSet(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
        messages += topic -> set
        producer.send(topic, set)
        set.buffer.rewind
        fetches += new FetchRequest(topic, 0, 0, 10000)
      }

      // wait a bit for produced message to be available
      Thread.sleep(200)
      val response = consumer.multifetch(fetches: _*)
      for((topic, resp) <- topics.zip(response.toList))
    	  TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
    }

    {
      // send some invalid offsets
      val fetches = new mutable.ArrayBuffer[FetchRequest]
      for(topic <- topics)
        fetches += new FetchRequest(topic, 0, -1, 10000)

      try {
        val responses = consumer.multifetch(fetches: _*)
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
        val responses = consumer.multifetch(fetches: _*)
        for(resp <- responses)
    	    resp.iterator
        fail("expect exception")
      }
      catch {
        case e: WrongPartitionException => "this is good"
      }
    }

  }

  def testProduceAndMultiFetchJava() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    {
      val messages = new mutable.HashMap[String, ByteBufferMessageSet]
      val fetches : java.util.ArrayList[FetchRequest] = new java.util.ArrayList[FetchRequest]
      for(topic <- topics) {
        val set = new ByteBufferMessageSet(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
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

  def testMultiProduce() {
    // send some messages
    val topics = List("test1", "test2", "test3");
    val messages = new mutable.HashMap[String, ByteBufferMessageSet]
    val fetches = new mutable.ArrayBuffer[FetchRequest]
    var produceList: List[ProducerRequest] = Nil
    for(topic <- topics) {
      val set = new ByteBufferMessageSet(new Message(("a_" + topic).getBytes), new Message(("b_" + topic).getBytes))
      messages += topic -> set
      produceList ::= new ProducerRequest(topic, 0, set)
      fetches += new FetchRequest(topic, 0, 0, 10000)
    }
    producer.multiSend(produceList.toArray)

    for (messageSet <- messages.values)
      messageSet.buffer.rewind
      
    // wait a bit for produced message to be available
    Thread.sleep(200)
    val response = consumer.multifetch(fetches: _*)
    for((topic, resp) <- topics.zip(response.toList))
  	  TestUtils.checkEquals(messages(topic).iterator, resp.iterator)
  }

  def testGetOffsets() {
    // send some messages
    val topic = "test"
    val sent = new ByteBufferMessageSet(new Message("hello".getBytes()), new Message("there".getBytes()))
    producer.send(topic, sent)

    // wait a bit until the log file is created
    Thread.sleep(100)
    val now = System.currentTimeMillis
    val actualOffsets1 = consumer.getOffsetsBefore(topic, 0, now, 10)
    val expectedOffsets1 = Array(28L)
    TestUtils.checkEquals(actualOffsets1.iterator, expectedOffsets1.iterator)

    val oldTime = now - 1000000
    val actualOffsets2 = consumer.getOffsetsBefore(topic, 0, oldTime, 10)
    val expectedOffsets2 = new Array[Long](0)
    TestUtils.checkEquals(actualOffsets2.iterator, expectedOffsets2.iterator)
  }
}
