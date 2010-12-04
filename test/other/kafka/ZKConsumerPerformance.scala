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

import kafka.utils.Utils
import kafka.consumer.{ConsumerConfig, ConsumerConnector, Consumer}
import java.util.concurrent.CountDownLatch
import java.nio.channels.ClosedByInterruptException
import org.apache.log4j.Logger
import java.util.concurrent.atomic.AtomicLong

abstract class ShutdownableThread(name: String) extends Thread(name) {
  def shutdown(): Unit  
}

object ZKConsumerPerformance {
  private val logger = Logger.getLogger(getClass())

  def main(args: Array[String]): Unit = {
    if(args.length < 5) {
      println("USAGE: " + ZKConsumerPerformance.getClass.getName + " consumer.properties topic #threads printInterval initialSleep")
      System.exit(1)
    }
    println("Starting consumer...")
    var totalNumMsgs = new AtomicLong(0)
    var totalNumBytes = new AtomicLong(0)
    
    val consumerConfig = new ConsumerConfig(Utils.loadProps(args(0)))
    val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
    val topic = args(1)
    val nThreads = args(2).toInt
    val printInterval = args(3).toInt
    val initialSleep = args(4).toInt

    val topicMessageStreams = consumerConnector.createMessageStreams(Predef.Map(topic -> nThreads))
    var threadList = List[ShutdownableThread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= new ShutdownableThread("kafka-zk-consumer-" + i) {
          private val shutdownLatch = new CountDownLatch(1)

          def shutdown(): Unit = {
            interrupt
            shutdownLatch.await
          }

          override def run() {
            var totalBytesRead = 0L
            var nMessages = 0L
            val startMs = System.currentTimeMillis

            try {
              for (message <- streamList(i)) {
                nMessages += 1
                totalBytesRead += message.payloadSize
                if (nMessages % printInterval == 0) {
                  val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
                  printMessage(totalBytesRead, nMessages, elapsedSecs)
                }
              }
            }
            catch {
              case _: InterruptedException =>
              case _: ClosedByInterruptException =>
              case e => throw e
            }
            totalNumMsgs.addAndGet(nMessages)
            totalNumBytes.addAndGet(totalBytesRead)
            val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
            printMessage(totalBytesRead, nMessages, elapsedSecs)
            shutdownComplete
          }

          private def printMessage(totalBytesRead: Long, nMessages: Long, elapsedSecs: Double) = {
            logger.info("thread[" + i + "], nMsgs:" + nMessages + " bytes:" + totalBytesRead +
              " nMsgs/sec:" + (nMessages / elapsedSecs).formatted("%.2f") +
              " MB/sec:" + (totalBytesRead / elapsedSecs / (1024.0*1024.0)).formatted("%.2f"))

          }
          private def shutdownComplete() = shutdownLatch.countDown
        }

    Thread.sleep(initialSleep)
    logger.info("starting threads")
    for (thread <- threadList)
      thread.start

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        for (thread <- threadList)
          thread.shutdown

        try {
          consumerConnector.shutdown
        }
        catch {
          case _ =>
        }
        println("total nMsgs: " + totalNumMsgs)
        println("totalBytesRead " + totalNumBytes)
      }
    });

  }

}