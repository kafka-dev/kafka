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

import java.util.Properties
import org.apache.log4j.{Logger, Level}
import kafka.server.{KafkaRequestHandlers, KafkaServer, KafkaConfig}
import kafka.zk.EmbeddedZookeeper
import kafka.{TestZKUtils, TestUtils}
import kafka.message.{ByteBufferMessageSet, Message}
import org.junit.{After, Before}
import junit.framework.{Assert, TestCase}
import kafka.serializer.Encoder

class RichProducerTest extends TestCase {
  private val topic = "test-topic"
  private val port1 = 9092
  private val port2 = 9093
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var zkServer:EmbeddedZookeeper = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  @Before
  override def setUp() {
    // set up 2 brokers with 4 partitions each
    super.setUp()
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)

    val props1 = TestUtils.createBrokerConfig(0, port1)
    val config1 = new KafkaConfig(props1) {
      override val numPartitions = 4
    }
    server1 = TestUtils.createServer(config1)

    val props2 = TestUtils.createBrokerConfig(1, port2)
    val config2 = new KafkaConfig(props2) {
      override val numPartitions = 4
    }
    server2 = TestUtils.createServer(config2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port1.toString)

    var producer = new SyncProducer(new SyncProducerConfig(props))
    producer.send("test-topic", new ByteBufferMessageSet(new Message("test".getBytes())))

    producer = new SyncProducer(new SyncProducerConfig(props) {
      override val port = port2
    })
    producer.send("test-topic", new ByteBufferMessageSet(new Message("test".getBytes())))

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    Thread.sleep(1000)
  }

  @After
  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)

    super.tearDown()
    server1.shutdown
    server2.shutdown
    zkServer.shutdown
    Thread.sleep(1000)
  }

  def testSend() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new RichProducerConfig(props)

    val richProducer = new RichProducer[String, String](config)
    richProducer.send(topic, "test", "test")

  }

  def testInvalidPartition() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new RichProducerConfig(props)

    val richProducer = new RichProducer[String, String](config)
    try {
      richProducer.send(topic, "test", "test")
      Assert.fail("Should fail with InvalidPartitionException")
    }catch {
      case e: InvalidPartitionException => 
    }
  }
}

class StringSerializer extends Encoder[String] {
  def toEvent(message: Message):String = message.toString
  def toMessage(event: String):Message = new Message(event.getBytes)
  def getTopic(event: String): String = event.concat("-topic")
}

class NegativePartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    -1
  }
}

class StaticPartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    (data.length % numPartitions)
  }
}

class HashPartitioner extends Partitioner[String] {
  def partition(data: String, numPartitions: Int): Int = {
    (data.hashCode % numPartitions)
  }
}