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

object TestRichConsumer {

  def main(args: Array[String]) {
    if(args.length < 3) {
      println("USAGE: " + getClass.getName + " kafka.properties urls topic ")
      System.exit(1)
    }
    println("Starting consumer...")
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val urls = args(1).split(",")
    val topic = args(2)
    val nodeToUrl: Map[Int, String] = Map(0.until(urls.size).toArray.zip(urls).toList: _*)
    //val pool = new ConsumerPool(nodeToUrl, 10000, 1000000, 1000)
    //val offsetStorage = new MemoryOffsetStorage
    //val backoffMs = 10000L
    //val messageStream = new KafkaMessageStream(pool, topic, 1000000, 3000000, 300, offsetStorage, backoffMs, true)
    //var count = 0
    //for(message <- messageStream)
    //  println(Utils.toString(message.payload, "UTF-8"))
    1
  }
  
}