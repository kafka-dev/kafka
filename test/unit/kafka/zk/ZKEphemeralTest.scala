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

package kafka.zk

import junit.framework.TestCase
import kafka.TestUtils
import kafka.server.KafkaServer
import kafka.consumer.ConsumerConfig
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{ZkUtils, StringSerializer}
import org.junit.Assert

class ZKEphemeralTest extends TestCase {

  private var server: KafkaServer = null
  private val zkConnect = "127.0.0.1:2181"

  def testEphemeralNodeCleanup = {
    val config = new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "test", "1"))
    var zkClient = new ZkClient(zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                                StringSerializer)

    try {
      ZkUtils.createEphemeralPathExpectConflict(zkClient, "/tmp/zktest", "node created")
    } catch {                       
      case e: Exception => println("Exception in creating ephemeral node")
    }

    var testData: String = null

    testData = ZkUtils.readData(zkClient, "/tmp/zktest")
    Assert.assertNotNull(testData)

    zkClient.close

    zkClient = new ZkClient(zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs,
                                StringSerializer)

    val nodeExists = ZkUtils.pathExists(zkClient, "/tmp/zktest")
    Assert.assertFalse(nodeExists)
  }
}