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

import async.{AsyncProducerConfig, AsyncProducer}
import java.util.Properties
import org.apache.log4j.{Logger, Level}
import kafka.server.{KafkaRequestHandlers, KafkaServer, KafkaConfig}
import kafka.zk.EmbeddedZookeeper
import kafka.{TestZKUtils, TestUtils}
import kafka.message.{ByteBufferMessageSet, Message}
import org.junit.{After, Before, Test}
import junit.framework.Assert
import kafka.serializer.Encoder
import collection.mutable.HashMap
import org.easymock.EasyMock
import kafka.utils.Utils
import java.util.concurrent.ConcurrentHashMap
import kafka.cluster.Partition
import kafka.common.{UnavailableProducerException, InvalidPartitionException}
import org.scalatest.junit.JUnitSuite

class ProducerTest extends JUnitSuite {
  private val topic = "test-topic"
  private val brokerId1 = 0
  private val brokerId2 = 1  
  private val port1 = 9092
  private val port2 = 9093
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var producer1: SyncProducer = null
  private var producer2: SyncProducer = null
  private var zkServer:EmbeddedZookeeper = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandlers])

  @Before
  def setUp() {
    // set up 2 brokers with 4 partitions each
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)

    val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
    val config1 = new KafkaConfig(props1) {
      override val numPartitions = 4
    }
    server1 = TestUtils.createServer(config1)

    val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
    val config2 = new KafkaConfig(props2) {
      override val numPartitions = 4
    }
    server2 = TestUtils.createServer(config2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port1.toString)

    producer1 = new SyncProducer(new SyncProducerConfig(props))
    producer1.send("test-topic", new ByteBufferMessageSet(new Message("test".getBytes())))

    producer2 = new SyncProducer(new SyncProducerConfig(props) {
      override val port = port2
    })
    producer2.send("test-topic", new ByteBufferMessageSet(new Message("test".getBytes())))

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)

    Thread.sleep(500)
  }

  @After
  def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    server1.shutdown
    server2.shutdown
    Utils.rm(server1.config.logDir)
    Utils.rm(server2.config.logDir)    
    Thread.sleep(500)
    zkServer.shutdown
  }

  @Test
  def testSend() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[SyncProducer])
    // it should send to partition 0 (first partition) on second broker i.e broker2
    syncProducer2.send(topic, 0, new ByteBufferMessageSet(new Message("test1".getBytes)))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)

    val producerPool = new ProducerPool(config, serializer, syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String](topic, "test", Array("test1")))
    producer.close

    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testInvalidPartition() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)
    val config = new ProducerConfig(props)

    val richProducer = new Producer[String, String](config)
    try {
      richProducer.send(new ProducerData[String, String](topic, "test", Array("test")))
      Assert.fail("Should fail with InvalidPartitionException")
    }catch {
      case e: InvalidPartitionException => // expected, do nothing
    }
  }

  @Test
  def testSyncProducerPool() {
    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[SyncProducer])
    syncProducer1.send("test-topic", 0, new ByteBufferMessageSet(new Message("test1".getBytes)))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)

    // default for producer.type is "sync"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    producerPool.send(producerPool.getProducerPoolData("test-topic", new Partition(brokerId1, 0), Array("test1")))

    producerPool.close
    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testAsyncProducerPool() {
    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    asyncProducer1.send(topic, "test1", 0)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId1, asyncProducer1)
    asyncProducers.put(brokerId2, asyncProducer2)

    // change producer.type to "async"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      new ConcurrentHashMap[Int, SyncProducer](), asyncProducers)
    producerPool.send(producerPool.getProducerPoolData(topic, new Partition(brokerId1, 0), Array("test1")))

    producerPool.close
    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
  }

  @Test
  def testSyncUnavailableProducerException() {
    val syncProducers = new ConcurrentHashMap[Int, SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[SyncProducer])
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId2, syncProducer2)

    // default for producer.type is "sync"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    try {
      producerPool.send(producerPool.getProducerPoolData("test-topic", new Partition(brokerId1, 0), Array("test1")))
      Assert.fail("Should fail with UnavailableProducerException")
    }catch {
      case e: UnavailableProducerException => // expected
    }

    producerPool.close
    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
  }

  @Test
  def testAsyncUnavailableProducerException() {
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId2, asyncProducer2)

    // change producer.type to "async"
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.NegativePartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    val producerPool = new ProducerPool[String](new ProducerConfig(props), new StringSerializer,
      new ConcurrentHashMap[Int, SyncProducer](), asyncProducers)
    try {
      producerPool.send(producerPool.getProducerPoolData(topic, new Partition(brokerId1, 0), Array("test1")))
      Assert.fail("Should fail with UnavailableProducerException")
    }catch {
      case e: UnavailableProducerException => // expected
    }

    producerPool.close
    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
  }

  @Test
  def testConfigBrokerPartitionInfo() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("broker.partition.info", brokerId1 + ":" + "localhost" + ":" + port1 + ":" + 4 + "," +
                                       brokerId2 + ":" + "localhost" + ":" + port2 + ":" + 4)
    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    // it should send to partition 0 (first partition) on second broker i.e broker2
    asyncProducer2.send(topic, "test1", 1)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId1, asyncProducer1)
    asyncProducers.put(brokerId2, asyncProducer2)

    val producerPool = new ProducerPool(config, serializer, new ConcurrentHashMap[Int, SyncProducer](), asyncProducers)
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String](topic, "test1", Array("test1")))
    producer.close

    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
  }

  @Test
  def testTopicBasedPartitionsInConfigBrokerPartitionInfo() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("broker.partition.info", brokerId1 + ":" + "localhost" + ":" + port1 + ":" + 4 + "," +
                                       brokerId2 + ":" + "localhost" + ":" + port2 + ":" + 4)
    props.put("topic.num.partitions", topic + ":8")
    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    // it should send to partition 0 (first partition) on second broker i.e broker2
    asyncProducer1.send(topic, "test1", 0)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId1, asyncProducer1)
    asyncProducers.put(brokerId2, asyncProducer2)

    val producerPool = new ProducerPool(config, serializer, new ConcurrentHashMap[Int, SyncProducer](), asyncProducers)
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String](topic, "testtesttesttest", Array("test1")))
    producer.close

    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
  }

  @Test
  def testPartitionedSendToNewTopic() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[SyncProducer])
    syncProducer1.send("test-topic1", 0, new ByteBufferMessageSet(new Message("test1".getBytes)))
    EasyMock.expectLastCall
    syncProducer1.send("test-topic1", 1, new ByteBufferMessageSet(new Message("test1".getBytes)))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)

    val producerPool = new ProducerPool(config, serializer, syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String]("test-topic1", "test", Array("test1")))

    // now send again to this topic using a real producer, this time all brokers would have registered
    // their partitions in zookeeper
    producer1.send("test-topic1", new ByteBufferMessageSet(new Message("test".getBytes())))

    // wait for zookeeper to register the new topic
    Thread.sleep(500)

    producer.send(new ProducerData[String, String]("test-topic1", "test1", Array("test1")))
    producer.close

    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)

  }

  @Test
  def testPartitionedSendToNewBrokerInExistingTopic() {
    val props = new Properties()
    props.put("partitioner.class", "kafka.producer.StaticPartitioner")
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("zk.connect", TestZKUtils.zookeeperConnect)

    val config = new ProducerConfig(props)
    val partitioner = new StaticPartitioner
    val serializer = new StringSerializer

    // 2 sync producers
    val syncProducers = new ConcurrentHashMap[Int, SyncProducer]()
    val syncProducer1 = EasyMock.createMock(classOf[SyncProducer])
    val syncProducer2 = EasyMock.createMock(classOf[SyncProducer])
    val syncProducer3 = EasyMock.createMock(classOf[SyncProducer])
    syncProducer3.send("test-topic", 2, new ByteBufferMessageSet(new Message("test1".getBytes)))
    EasyMock.expectLastCall
    syncProducer1.close
    EasyMock.expectLastCall
    syncProducer2.close
    EasyMock.expectLastCall
    syncProducer3.close
    EasyMock.expectLastCall
    EasyMock.replay(syncProducer1)
    EasyMock.replay(syncProducer2)
    EasyMock.replay(syncProducer3)

    syncProducers.put(brokerId1, syncProducer1)
    syncProducers.put(brokerId2, syncProducer2)
    syncProducers.put(2, syncProducer3)

    val producerPool = new ProducerPool(config, serializer, syncProducers, new ConcurrentHashMap[Int, AsyncProducer[String]]())
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    val serverProps = TestUtils.createBrokerConfig(2, 9094)
    val serverConfig = new KafkaConfig(serverProps) {
      override val numPartitions = 4
    }
    val server3 = TestUtils.createServer(serverConfig)

    // send a message to the new broker to register it under topic "test-topic"
    val tempProps = new Properties()
    tempProps.put("host", "localhost")
    tempProps.put("port", "9094")
    val tempProducer = new SyncProducer(new SyncProducerConfig(tempProps))
    tempProducer.send("test-topic", new ByteBufferMessageSet(new Message("test".getBytes())))

    Thread.sleep(500)

    producer.send(new ProducerData[String, String]("test-topic", "test-topic", Array("test1")))
    producer.close

    EasyMock.verify(syncProducer1)
    EasyMock.verify(syncProducer2)
    EasyMock.verify(syncProducer3)

    server3.shutdown
    Utils.rm(server3.config.logDir)
  }

  @Test
  def testDefaultPartitioner() {
    val props = new Properties()
    props.put("serializer.class", "kafka.producer.StringSerializer")
    props.put("producer.type", "async")
    props.put("broker.partition.info", brokerId1 + ":" + "localhost" + ":" + port1 + ":" + 4 + "," +
                                       brokerId2 + ":" + "localhost" + ":" + port2 + ":" + 4)
    val config = new ProducerConfig(props)
    val partitioner = new DefaultPartitioner[String]
    val serializer = new StringSerializer

    // 2 async producers
    val asyncProducers = new ConcurrentHashMap[Int, AsyncProducer[String]]()
    val asyncProducer1 = EasyMock.createMock(classOf[AsyncProducer[String]])
    val asyncProducer2 = EasyMock.createMock(classOf[AsyncProducer[String]])
    // it should send to partition 0 (first partition) on second broker i.e broker2
    asyncProducer1.send(topic, "test1", 2)
    EasyMock.expectLastCall
    asyncProducer1.close
    EasyMock.expectLastCall
    asyncProducer2.close
    EasyMock.expectLastCall
    EasyMock.replay(asyncProducer1)
    EasyMock.replay(asyncProducer2)

    asyncProducers.put(brokerId1, asyncProducer1)
    asyncProducers.put(brokerId2, asyncProducer2)

    val producerPool = new ProducerPool(config, serializer, new ConcurrentHashMap[Int, SyncProducer](), asyncProducers)
    val producer = new Producer[String, String](config, partitioner, producerPool, false)

    producer.send(new ProducerData[String, String](topic, "test", Array("test1")))
    producer.close

    EasyMock.verify(asyncProducer1)
    EasyMock.verify(asyncProducer2)
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
