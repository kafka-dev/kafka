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

package kafka

import api.{FetchRequest, OffsetRequest}
import consumer.SimpleConsumer
import java.util.Properties
import server.KafkaConfig
import log.Log
import utils.Utils
import java.io.File

object TestLatestLogOffset {
  def main(args: Array[String]) {
    val brokerPort = 9092
    
    if(args.length < 3)
      Utils.croak("USAGE: java " + getClass().getName() + " log_dir topic partition")
    val logDirName = args(0)
    val topic = args(1)
    val partition = args(2).toInt

    val topicDir = logDirName + "/" + topic + "-" + partition
    val log = new Log(new File(topicDir), 50*1024*1024, 5000000)
    val offsets = log.getOffsetsBefore(new OffsetRequest(topic, partition, OffsetRequest.LATEST_TIME, 1))
    println("# of latest offsets returned " + offsets.length)

    if(offsets.length > 0) {
      val latestOffset = offsets.last
      println("Latest offset according to getOffsetsBefore API = " + latestOffset)
    }

    val earliestOffsets = log.getOffsetsBefore(new OffsetRequest(topic, partition, OffsetRequest.EARLIEST_TIME, 1))
    println("# of earliest offsets returned " + earliestOffsets.length)

    if(earliestOffsets.length > 0) {
      val earliestOffset = earliestOffsets.last
      println("Earliest offset according to getOffsetsBefore API = " + earliestOffset)
    }

    val config: Properties = createBrokerConfig(1, brokerPort, logDirName)

    val server = TestUtils.createServer(new KafkaConfig(config))
    
    val simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024)
    
    val messageSet = simpleConsumer.fetch(new FetchRequest(topic, 0, 0, 1024))
    println("Message set length : " + messageSet.iterator.hasNext)
  }

  def createBrokerConfig(brokerId: Int, port: Int, logDir: String): Properties = {
    val props = new Properties
    props.put("broker.id", brokerId.toString)
    props.put("port", port.toString)
    props.put("log.dir", logDir)
    props.put("log.flush.interval", "1")
    props.put("enable.zookeeper", "false")
    props    
  }
}