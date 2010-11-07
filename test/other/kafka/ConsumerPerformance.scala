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

import kafka.utils._
import kafka.server._
import kafka.consumer.SimpleConsumer

object ConsumerPerformance {

    def main(args: Array[String]) {
    if(args.length < 4) {
      println("USAGE: " + ConsumerPerformance.getClass.getName + " kafka.properties host topic fetch_size")
      System.exit(1)
    }
    
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val host = args(1)
    val topic = args(2)
    val fetchSize = args(3).toInt
    
    val consumer = new SimpleConsumer(host, config.port, 2*fetchSize, 1024*1024)
    val startMs = System.currentTimeMillis
    var done = false
    var totalRead = 0
    val reportingInterval = 100000
    var consumedInInterval = 0
    var offset: Long = 0L
    while(!done) {
      val messages = consumer.fetch(topic, offset, fetchSize)
      var messagesRead = 0
      for(message <- messages)
        messagesRead += 1
      
      if(messagesRead == 0)
        done = true
      else
        offset += messages.validBytes
      
      totalRead += messagesRead
      consumedInInterval += messagesRead
      
      if(consumedInInterval > reportingInterval) {
        println(totalRead)
        consumedInInterval = 0
      }
    }
    val ellapsedSeconds = (System.currentTimeMillis - startMs) / 1000.0
    println(totalRead + " messages read, " + offset + " bytes")
    println("Messages/sec: " + totalRead / ellapsedSeconds)
    println("MB/sec: " + offset / ellapsedSeconds / (1024.0*1024.0))
    System.exit(0)
  }
  
}
