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

package kafka.network

import java.util.concurrent.atomic._
import javax.management._
import kafka.utils._
import kafka.api.RequestKeys

trait SocketServerStatsMBean {
  def getProduceRequestsPerSecond: Double
  def getFetchRequestsPerSecond: Double
  def getAvgProduceRequestMs: Double
  def getAvgFetchRequestMs: Double
  def getBytesReadPerSecond: Double
  def getBytesWrittenPerSecond: Double
  def getNumFetchRequests: Long
  def getNumProduceRequests: Long
}

@threadsafe
class SocketServerStats(val monitorDurationNs: Long, val time: Time) extends SocketServerStatsMBean {
  
  def this(monitorDurationNs: Long) = this(monitorDurationNs, SystemTime)
  
  val complete = new AtomicReference(new Stats())
  val current = new AtomicReference(new Stats())
  val numCumulatedProduceRequests = new AtomicLong(0)
  val numCumulatedFetchRequests = new AtomicLong(0)

  def recordRequest(requestTypeId: Short, durationNs: Long) {
    val stats = current.get
    requestTypeId match {
      case r if r == RequestKeys.Produce || r == RequestKeys.MultiProduce =>
        stats.totalProduceRequestTimeNs.getAndAdd(durationNs)
        stats.numProduceRequests.getAndIncrement
        numCumulatedProduceRequests.getAndIncrement
      case r if r == RequestKeys.Fetch || r == RequestKeys.MultiFetch =>
        stats.totalFetchRequestTimeNs.getAndAdd(durationNs)
        stats.numFetchRequests.getAndIncrement
        numCumulatedFetchRequests.getAndIncrement
      case _ => /* not collecting; let go */
    }
    val ageNs = time.nanoseconds - stats.start
    // if the current stats are too old it is time to swap
    if(ageNs >= monitorDurationNs) {
      val swapped = current.compareAndSet(stats, new Stats())
      if(swapped) {
        complete.set(stats)
        stats.end.set(time.nanoseconds)
      }
    }
  }
  
  def recordBytesWritten(bytes: Int): Unit = 
    current.get.bytesWritten.addAndGet(bytes)
  
  def recordBytesRead(bytes: Int): Unit = 
    current.get.bytesRead.addAndGet(bytes)

  def getProduceRequestsPerSecond: Double = {
    val stats = complete.get
    stats.numProduceRequests.get / stats.durationSeconds
  }
  
  def getFetchRequestsPerSecond: Double = {
    val stats = complete.get
    stats.numFetchRequests.get / stats.durationSeconds
  }

  def getAvgProduceRequestMs: Double = {
    val stats = complete.get
    stats.totalProduceRequestTimeNs.get / stats.numProduceRequests.get / (1000.0 * 1000.0)
  }
  
  def getAvgFetchRequestMs: Double = {
    val stats = complete.get
    stats.totalFetchRequestTimeNs.get / stats.numFetchRequests.get / (1000.0 * 1000.0)
  }

  def getBytesReadPerSecond: Double = {
    val stats = complete.get
    stats.bytesRead.get / stats.durationSeconds
  }
  
  def getBytesWrittenPerSecond: Double = {
    val stats = complete.get
    stats.bytesWritten.get / stats.durationSeconds
  }

  def getNumFetchRequests: Long = numCumulatedFetchRequests.get

  def getNumProduceRequests: Long = numCumulatedProduceRequests.get

  class Stats {
    val start = time.nanoseconds
    var end = new AtomicLong(-1)
    val numFetchRequests = new AtomicInteger(0)
    val numProduceRequests = new AtomicInteger(0)
    val totalFetchRequestTimeNs = new AtomicLong(0)
    val totalProduceRequestTimeNs = new AtomicLong(0)
    val bytesRead = new AtomicInteger(0)
    val bytesWritten = new AtomicInteger(0)
    
    def durationSeconds: Double = (end.get - start) / (1000.0 * 1000.0 * 1000.0)
    
    def durationMs: Double = (end.get - start) / (1000.0 * 1000.0)
  }
  
}
