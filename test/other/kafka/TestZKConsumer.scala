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

import consumer.{KafkaMessageStream, ConsumerConnector, Consumer, ConsumerConfig}
import kafka.utils.Utils
import java.util.concurrent.CountDownLatch

object TestZKConsumer {
  def main(args: Array[String]): Unit = {
    if(args.length < 3) {
      println("USAGE: " + TestZKConsumer.getClass.getName + " consumer.properties topic #partitions")
      System.exit(1)
    }
    println("Starting consumer...")

    val consumerConfig = new ConsumerConfig(Utils.loadProps(args(0)))
    val topic = args(1)
    val nParts = args(2).toInt
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topic -> nParts))
    var threadList = List[ZKConsumerThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (stream <- streamList)
        threadList ::= new ZKConsumerThread(stream)

    for (thread <- threadList)
      thread.start

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        consumerConnector.shutdown
        threadList.foreach(_.shutdown)
        println("consumer threads shutted down")        
      }
    })
  }
}

class ZKConsumerThread(stream: KafkaMessageStream) extends Thread {
  val shutdownLatch = new CountDownLatch(1)

  override def run() {
    println("Starting consumer thread..")
    for (message <- stream) {
      println("consumed: " + Utils.toString(message.payload, "UTF-8"))
    }
    shutdownLatch.countDown
    println("thread shutdown !" )
  }

  def shutdown() {
    shutdownLatch.await
  }          
}