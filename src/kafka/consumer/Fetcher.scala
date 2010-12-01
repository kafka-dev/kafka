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

package kafka.consumer

import scala.collection._
import org.apache.log4j.Logger
import kafka.cluster._
import org.I0Itec.zkclient.ZkClient


/**
 * The fetcher is a background thread that fetches data from a set of servers
 */
class Fetcher(val config: ConsumerConfig, val zkClient : ZkClient) {
  private val logger = Logger.getLogger(getClass())
  private val EMPTY_FETCHER_THREADS = new Array[FetcherRunnable](0)
  @volatile
  private var fetcherThreads : Array[FetcherRunnable] = EMPTY_FETCHER_THREADS
  private var currentTopicInfos: Iterable[PartitionTopicInfo] = null
  
  /**
   *  shutdown all fetch threads
   */
  def shutdown() {
    // shutdown the old fetcher threads, if any
    for (fetcherThread <- fetcherThreads)
      fetcherThread.shutdown
    fetcherThreads = EMPTY_FETCHER_THREADS
  }

  def clearAllQueues(topicInfos: Iterable[PartitionTopicInfo]) = topicInfos.foreach(_.chunkQueue.clear)

  /**
   *  Open connections.
   */
  def initConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster) {
    shutdown

    if (topicInfos == null)
      return

    if (currentTopicInfos != null)
      clearAllQueues(currentTopicInfos)
    currentTopicInfos = topicInfos
    
    // re-arrange by broker id
    val m = new mutable.HashMap[Int, List[PartitionTopicInfo]]
    for(info <- topicInfos) {
      m.get(info.brokerId) match {
        case None => m.put(info.brokerId, List(info))
        case Some(lst) => m.put(info.brokerId, info :: lst)
      }
    }

    // open a new fetcher thread for each broker
    val ids = Set() ++ topicInfos.map(_.brokerId)
    val brokers = ids.map(cluster.getBroker(_))
    fetcherThreads = new Array[FetcherRunnable](brokers.size)
    var i = 0
    for(broker <- brokers) {
      val fetcherThread = new FetcherRunnable("FetchRunnable-" + i, zkClient, config, broker, m.get(broker.id).get)
      fetcherThreads(i) = fetcherThread
      fetcherThread.start
      i +=1
    }
  }    
}


