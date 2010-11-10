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

import java.util.concurrent._
import java.util.concurrent.atomic._
import kafka.utils._
import kafka.server._
import kafka.message._
import kafka.producer._

/**
 * Load test for producer
 */
object ProducerPerformance {
  
  def main(args: Array[String]) {
    if(args.length < 9) {
      println("USAGE: " + ProducerPerformance.getClass.getName + " kafka.properties host num_messages message_size var/fix batch_size num_threads topic compression_enabled")
      System.exit(1)
    }
    
    val config = new KafkaConfig(Utils.loadProps(args(0)))
    val host = args(1)
    val numMessages = args(2).toInt
    val messageSize = args(3).toInt
    var isFixSize = if (args(4).compareToIgnoreCase("fix") == 0) true else false
    val batchSize = args(5).toInt
    val numThreads = args(6).toInt
    val topic = args(7)
    val compressionEnabled = args(8).toBoolean
    val rand = new java.util.Random
    
    val producer = new KafkaProducer(host, config.port, config.socketSendBuffer, 30000, 100000)
    val batchesPerThread = numMessages / numThreads / batchSize / config.numPartitions
    val totalBytesSent = new AtomicLong(0)
    val totalBatchesSent = new AtomicLong(0)
    val reportingInterval = 1000
    val executor = Executors.newFixedThreadPool(numThreads)
    val allDone = new CountDownLatch(numThreads * config.numPartitions)
    val startMs = System.currentTimeMillis
    for (part <- 0 until config.numPartitions)
      for(i <- 0 until numThreads) {
        executor.execute(Utils.runnable( () => {
          var bytesSent = 0L
          var lastBytesSent = 0L
          var nSends = 0
          var lastNSends = 0
          var reportTime = System.currentTimeMillis()
          var lastReportTime = reportTime
          val messages = new Array[Message](batchSize)
          for(j <- 0 until batchesPerThread) {
            for(k <- 0 until batchSize) {
              var bytes: Array[Byte] = null
              if (isFixSize)
                bytes = new Array[Byte](messageSize)
              else {
                val varSize = rand.nextInt(messageSize)
                bytes = new Array[Byte](varSize)
                rand.nextBytes(bytes)
              }
              messages(k) = new Message(bytes)
              bytesSent += messages(k).payloadSize
            }
            val set = new ByteBufferMessageSet(compressionEnabled, messages: _*)
            val sendStart = System.currentTimeMillis
            try  {
              producer.send(topic, part, set)
            }catch {
              case e: Exception => e.printStackTrace
            }
            val sendEnd = System.currentTimeMillis
            println("Send took " + (sendEnd - sendStart) + " ms")
            nSends += 1
            if(nSends % reportingInterval == reportingInterval - 1) {
              reportTime = System.currentTimeMillis()
              println("thread " + i + ": "
                + (1000.0 * (nSends - lastNSends) * batchSize / (reportTime - lastReportTime)).formatted("%.4f") + " nMsg/sec "
                + ((1000.0 * bytesSent - lastBytesSent) / (reportTime - lastReportTime) / (1024 * 1024)).formatted("%.4f") + " MBs/sec")
              lastReportTime = reportTime
              lastBytesSent = bytesSent
              lastNSends = nSends
            }
          }
          totalBytesSent.addAndGet(bytesSent)
          totalBatchesSent.addAndGet(nSends)
          allDone.countDown()
        }))
      }
    
    allDone.await()
    val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
    println("Total Num Messages: " + totalBatchesSent.get * batchSize + " bytes: " + totalBytesSent.get + " in " + elapsedSecs + " secs")
    println("Messages/sec: " + (1.0 * totalBatchesSent.get * batchSize / elapsedSecs).formatted("%.4f"))
    println("MB/sec: " + (totalBytesSent.get / elapsedSecs / (1024.0*1024.0)).formatted("%.4f"))
    producer.close()
    System.exit(0)
  }

}
