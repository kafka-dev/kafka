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

import consumer.SimpleConsumer
import producer.KafkaProducer
import scala.io.Source
import java.io._
import server.{KafkaServer, KafkaConfig}
import utils.Utils

/**
 * A simple echo application that sends messages to kafka, and subscribes to the feed to get them back out
 */
object Echoer {

  def main(args: Array[String]) {
    if(args.length < 1) {
      println("USAGE: " + Echoer.getClass.getName + " kafka.properties")
      System.exit(1)
    }
    
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val server = new KafkaServer(config)
    server.startup()
    Thread.sleep(500)
    val producer = new KafkaProducer("localhost", config.port, config.socketSendBuffer, 1000000, 100)
    Thread.sleep(1000)
    val consumer = new SimpleConsumer("localhost", config.port, 10000, config.socketReceiveBuffer)
    Utils.daemonThread("kafka-consumer", () => {
        var offset = 0L
        while(true) {
          val messages = consumer.fetch("test", offset, 10000)
          println("fetched " + messages.sizeInBytes)
          for(message <- messages) {
            println("consumed: " + Utils.toString(message.payload, "UTF-8"))
          }
          offset += messages.validBytes
          Thread.sleep(3000)
        } 
    }).start();
    val input = new BufferedReader(new InputStreamReader(System.in))
    for(line <- Source.fromInputStream(System.in).getLines) {
      val lineBytes = line.getBytes()
      producer.send("test", TestUtils.singleMessageSet(lineBytes))
      println("sent: " + line)
    }
    producer.close()
  }
  
}
