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

import joptsimple.OptionParser
import kafka.message.{Message, ByteBufferMessageSet}
import kafka.utils.Utils
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicLong
import java.util.Properties
import kafka.producer._

/**
 * Load test for the producer
 */
object ProducerPerformance {
  
  def main(args: Array[String]) {
    
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zk.connect", "REQUIRED: zookeeper connection string.")
                           .withRequiredArg
                           .describedAs("hostname:port")
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
    val asyncOpt = parser.accepts("async", "If set, messages are sent asynchronously.")
    val delayMSBtwSendOpt = parser.accepts("delay-btw-send-ms", "Delay in ms between 2 batch sends.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(0)
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
    
    for(arg <- List(zkConnectOpt, topicOpt, numMessagesOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }
    
    val zkConnect = options.valueOf(zkConnectOpt)
    val numMessages = options.valueOf(numMessagesOpt).intValue
    val messageSize = options.valueOf(messageSizeOpt).intValue
    val isFixSize = !options.has(varyMessageSizeOpt)
    val isAsync = options.has(asyncOpt)
    val delayedMSBtwSend = options.valueOf(delayMSBtwSendOpt).longValue
    val batchSize = options.valueOf(batchSizeOpt).intValue
    val numThreads = options.valueOf(numThreadsOpt).intValue
    val topic = options.valueOf(topicOpt)
    val partitions = options.valueOf(numPartitionsOpt).intValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).intValue
    val rand = new java.util.Random

    val props = new Properties()
    props.put("zk.connect", zkConnect)
    if (isAsync)
      props.put("producer.type","async")
    else {
      props.put("producer.type", "sync")
      props.put("batch.size", batchSize.toString)
      props.put("event.handler", "kafka.producer.async.EventHandler")
    }
    val producer = new Producer[String, String](new ProducerConfig(props))
    val messagesPerThread = numMessages / numThreads
    val totalBytesSent = new AtomicLong(0)
    val totalMessagesSent = new AtomicLong(0)
    val executor = Executors.newFixedThreadPool(numThreads)
    val allDone = new CountDownLatch(numThreads)
    val startMs = System.currentTimeMillis
    for(i <- 0 until numThreads) {
      executor.execute(Utils.runnable( () => {
        var bytesSent = 0L
        var lastBytesSent = 0L
        var nSends = 0
        var lastNSends = 0
        var reportTime = System.currentTimeMillis()
        var lastReportTime = reportTime
        val messages = new Array[Message](batchSize)
        for(j <- 0 until messagesPerThread) {
          var strLength = messageSize
          if (!isFixSize)
            strLength = rand.nextInt(messageSize)
          val message = getStringOfLength(strLength)
          bytesSent += strLength
          try  {
            producer.send(new ProducerData[String,String](topic, message))
            if (delayedMSBtwSend > 0)
              Thread.sleep(delayedMSBtwSend)
          }catch {
            case e: Exception => e.printStackTrace
          }
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
        totalMessagesSent.addAndGet(nSends)
        allDone.countDown()
        }))
      }
    
    allDone.await()
    val elapsedSecs = (System.currentTimeMillis - startMs) / 1000.0
    println("Total Num Messages: " + totalMessagesSent.get + " bytes: " + totalBytesSent.get + " in " + elapsedSecs + " secs")
    println("Messages/sec: " + (1.0 * totalMessagesSent.get / elapsedSecs).formatted("%.4f"))
    println("MB/sec: " + (totalBytesSent.get / elapsedSecs / (1024.0*1024.0)).formatted("%.4f"))
    producer.close()
    System.exit(0)
  }

  private def getStringOfLength(len: Int) : String = {
    val strArray = new Array[Char](len)
    for (i <- 0 until len)
      strArray(i) = 'x'
    return new String(strArray)
  }
}
