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

import java.io._
import kafka.utils._
import kafka.server._
import kafka.producer._

object TestProducer {
  def main(args: Array[String]) {
    if(args.length < 2) {
      println("USAGE: " + TestProducer.getClass.getName + " kafka.properties topic")
      System.exit(1)
    }
    
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val topic = args(1)
    val producer = new SimpleProducer("localhost", config.port, config.socketSendBuffer, 1000000, 100)
    
    val input = new BufferedReader(new InputStreamReader(System.in))
    while(true) {
      val line = input.readLine().trim()
      val lineBytes = line.getBytes()
      val messages = TestUtils.singleMessageSet(lineBytes)
      producer.send(topic, messages)
      println("sent: " + line + " (" + messages.sizeInBytes + " bytes)")
    }
    producer.close()
  }
}
