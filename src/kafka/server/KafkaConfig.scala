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

package kafka.server

import java.util.Properties
import kafka.message._
import kafka.utils._

/**
 * Configuration settings for the kafka server
 */
class KafkaConfig(props: Properties) extends ZKConfig(props) {
  /* the port to listen and accept connections on */
  val port: Int = Utils.getInt(props, "port", 6667)

  /* hostname of broker. If not set, will pick up from the value returned from getLocalHost. */
  val hostName: String = Utils.getString(props, "hostname", null)

  /* the broker id for this server */
  val brokerId: Int = Utils.getInt(props, "broker.id")
  
  /* the SO_SNDBUFF buffer of the socket sever sockets */
  val socketSendBuffer: Int = Utils.getInt(props, "socket.send.buffer", 100*1024)
  
  /* the SO_RCVBUFF buffer of the socket sever sockets */
  val socketReceiveBuffer: Int = Utils.getInt(props, "socket.receive.buffer", 100*1024)
  
  /* the maximum number of bytes in a socket request */
  val maxSocketRequestSize: Int = Utils.getIntInRange(props, "max.socket.request.bytess", 100*1024*1024, (1, Int.MaxValue))
  
  /* the number of threads the server uses */
  val numThreads = Utils.getIntInRange(props, "num.threads", Runtime.getRuntime().availableProcessors, (1, Int.MaxValue))
  
  /* the interval in which to measure performance statistics */
  val monitoringPeriodSecs = Utils.getIntInRange(props, "monitoring.period.secs", 30, (1, Int.MaxValue))
  
  /* the number of log partitions per topic */
  val numPartitions = Utils.getIntInRange(props, "num.partitions", 1, (1, Int.MaxValue))
  
  /* the directory in whcih the log data is kept */
  val logDir = Utils.getString(props, "log.dir")
  
  /* the maximum size of a single log file */
  val logFileSize = Utils.getIntInRange(props, "log.file.size", 1*1024*1024*1024, (Message.HeaderSize, Int.MaxValue))
  
  /* the number of messages to accept without flushing the log to disk */
  val flushInterval = Utils.getIntInRange(props, "log.flush.interval", 1, (1, Int.MaxValue))
  
  /* the number of hours to keep log data before deleting it */
  val logRetentionHours = Utils.getIntInRange(props, "log.retention.hours", 24 * 7, (1, Int.MaxValue))
  
  /* the number of minutes between checking for logs eligible for deletion */
  val logCleanupIntervalMinutes = Utils.getIntInRange(props, "log.cleanup.interval.mins", 10, (1, Int.MaxValue))
  
  /* enable zookeeper registration in the server */
  val enableZookeeper = Utils.getBoolean(props, "enable.zookeeper", true)

  /* topic flush interval map topic_name => key, int => flush interval in seconds */
  val flushIntervalMap = Utils.getTopicFlushIntervals(Utils.getString(props, "topic.flush.intervals.ms", ""))

  /* default topic flush interval in ms  */
  val defaultflushIntervalMs = Utils.getInt(props, "log.default.flush.interval.ms", Int.MaxValue)

  /* default topic schduler flusher time interval to schedule flush on log files */
  val flushSchedulerThreadRate = Utils.getInt(props, "log.default.flush.scheduler.interval.ms",  5000)

   /* topic partition count map (topic_name => key, int => # of partitions) */
  val topicPartitionsMap = Utils.getTopicFlushIntervals(Utils.getString(props, "topic.partition.count.map", ""))
}
