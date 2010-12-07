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

import api.FetchRequest
import kafka.utils._
import kafka.consumer._
import kafka.server._

object TestConsumer {

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      println("USAGE: " + TestConsumer.getClass.getName + " kafka.properties topic")
      System.exit(1)
    }
    println("Starting consumer...")
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val topic = args(1)
    val consumer = new SimpleConsumer("localhost", config.port, 10000, config.socketReceiveBuffer)
    val thread = Utils.newThread("kafka-consumer", new Runnable() {
      def run() {
        var offset = 0L
        while(true) {
          val messages = consumer.fetch(new FetchRequest(topic, 0, offset, 10000))
          println("fetched " + messages.sizeInBytes + " bytes from offset " + offset)
          var consumed = 0
          for(message <- messages) {
            println("consumed: " + Utils.toString(message.payload, "UTF-8"))
            consumed += 1
          }
          if(consumed > 0)
            offset += messages.validBytes
          Thread.sleep(10000)
        }
      }  
    }, false);
    thread.start()
    thread.join()
  }
  
}
