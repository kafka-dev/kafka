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

import kafka.server._
import kafka.utils._
import kafka.message._
import kafka.producer._

object TestRichProducer {

  def main(args: Array[String]) {
    if(args.length < 5) {
      println("USAGE: " + getClass.getName + " kafka.properties host input_file topic sleep_time")
      System.exit(1)
    }
    
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val host = args(1)
    val input = args(2)
    val topic = args(3)
    val sleepTime = args(4).toInt
    
    val reader = new BufferedReader(new FileReader(input))
    val producer = new KafkaProducer(host, config.port, 100000, 10000, 10000)
    var count = 1
    var line: String = null
    val compressionEnabled = false
    do {
      line = reader.readLine()
      val message = new Message(line.getBytes("UTF-8"))
      producer.send(topic, new ByteBufferMessageSet(compressionEnabled, message))
      // rest for a bit
      if(sleepTime > 0)
        Thread.sleep(sleepTime)
      count += 1
      if(count % 100000 == 0)
        println(count)
    } while(line != null);
  }
}
