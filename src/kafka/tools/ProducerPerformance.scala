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

package kafka.tools

import java.net.URI
import java.util.concurrent._
import java.util.concurrent.atomic._
import joptsimple._
import kafka.utils._
import kafka.message._
import kafka.producer._

/**
 * Load test for the producer
 */
object ProducerPerformance {
  
  def main(args: Array[String]) {
    
    val parser = new OptionParser
    val urlOpt = parser.accepts("server", "REQUIRED: The hostname of the server to connect to.")
                           .withRequiredArg
                           .describedAs("kafka://hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to consume from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val numMessagesOpt = parser.accepts("messages", "REQUIRED: The number of messages to send.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
    val messageSizeOpt = parser.accepts("message-size", "The size of each message.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(100)
    val varyMessageSizeOpt = parser.accepts("vary-message-size", "If set, message size will vary up to the given maximum.")
    val batchSizeOpt = parser.accepts("batch-size", "Number of messages to send in a single batch.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(100)
    val numThreadsOpt = parser.accepts("threads", "Number of sending threads.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(10)
    val numPartitionsOpt = parser.accepts("partitions", "Number of sending partitions.")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    val reportingIntervalOpt = parser.accepts("reporting-interval", "Interval at which to print progress info.")
                           .withRequiredArg
                           .describedAs("size")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(100000)
    
    val options = parser.parse(args : _*)
    
    for(arg <- List(urlOpt, topicOpt, numMessagesOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val url = new URI(options.valueOf(urlOpt))
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val messageSize = options.valueOf(messageSizeOpt).intValue
    var isFixSize = options.has(varyMessageSizeOpt)
    val batchSize = options.valueOf(batchSizeOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val topic = options.valueOf(topicOpt)
    val partitions = options.valueOf(numPartitionsOpt).intValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val rand = new java.util.Random
    
    val producer = new SimpleProducer(url.getHost, url.getPort, 1*1024*1024, 30000, 100000)
    val batchesPerThread = numMessages / numThreads / batchSize / partitions
    val totalBytesSent = new AtomicLong(0)
    val totalBatchesSent = new AtomicLong(0)
    val executor = Executors.newFixedThreadPool(numThreads)
    val allDone = new CountDownLatch(numThreads * partitions)
    val startMs = System.currentTimeMillis
    for (part <- 0 until partitions)
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
            val set = new ByteBufferMessageSet(messages: _*)
            val sendStart = System.currentTimeMillis
            try  {
              producer.send(topic, part, set)
            }catch {
              case e: Exception => e.printStackTrace
            }
            val sendEnd = System.currentTimeMillis
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
