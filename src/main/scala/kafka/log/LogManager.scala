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

package kafka.log

import java.io._
import org.apache.log4j.Logger
import kafka.utils._
import scala.actors.Actor
import scala.actors.Actor._
import java.util.concurrent.CountDownLatch
import kafka.server.{KafkaConfig, KafkaZooKeeper}
import kafka.common.WrongPartitionException

/**
 * The guy who creates and hands out logs
 */
@threadsafe
class LogManager(val config: KafkaConfig,
                 private val scheduler: KafkaScheduler,
                 private val time: Time,
                 val logCleanupIntervalMs: Long,
                 val logCleanupMinAgeMs: Long) {
  
  val logDir: File = new File(config.logDir)
  private val numPartitions = config.numPartitions
  private val maxSize: Long = config.logFileSize
  private val flushInterval = config.flushInterval
  private val topicPartitionsMap = config.topicPartitionsMap
  private val logger = Logger.getLogger(classOf[LogManager])
  private val logCreationLock = new Object
  private val random = new java.util.Random
  private var kafkaZookeeper: KafkaZooKeeper = null
  private var zkActor: Actor = null
  private val startupLatch: CountDownLatch = if (config.enableZookeeper) new CountDownLatch(1) else null
  private val logFlusherScheduler = new KafkaScheduler(1, "kafka-logflusher-", false)
  private val logFlushIntervalMap = config.flushIntervalMap

  /* Initialize a log for each subdirectory of the main log directory */
  private val logs = new Pool[String, Pool[Int, Log]]()
  if(!logDir.exists()) {
    logger.info("No log directory found, creating '" + logDir.getAbsolutePath() + "'")
    logDir.mkdirs()
  }
  if(!logDir.isDirectory() || !logDir.canRead())
    throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log directory.")
  val subDirs = logDir.listFiles()
  if(subDirs != null) {
    for(dir <- subDirs) {
      if(!dir.isDirectory()) {
        logger.warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?")
      } else {
        logger.info("Loading log '" + dir.getName() + "'")
        val log = new Log(dir, maxSize, flushInterval)
        val topicPartion = Utils.getTopicPartition(dir.getName)
        logs.putIfNotExists(topicPartion._1, new Pool[Int, Log]())
        val parts = logs.get(topicPartion._1)
        parts.put(topicPartion._2, log)
      }
    }
  }
  
  // initialize jmx monitoring for the logs
  for(log <- getLogIterator)
    Utils.registerMBean(new LogStats(log), "kafka:type=kafka.logs." + log.dir.getName)
  
  /* Schedule the cleanup task to delete old logs */
  if(scheduler != null) {
    logger.info("starting log cleaner every " + logCleanupIntervalMs + " ms")    
    scheduler.scheduleWithRate(cleanupLogs, 60 * 1000, logCleanupIntervalMs)
  }



  if(config.enableZookeeper) {
    kafkaZookeeper = new KafkaZooKeeper(config, this)
    kafkaZookeeper.startup
    zkActor = new Actor {
      def act() {
        loop {
          receive {
            case topic: String =>
              try {
                kafkaZookeeper.registerTopicInZk(topic)
              }
              catch {
                case e => logger.error(e) // log it and let it go
              }
            case StopActor =>
              logger.info("zkActor stopped")
              exit
          }
        }
      }
    }
    zkActor.start
  }

  case object StopActor

  /**
   *  Register this broker in ZK for the first time.
   */
  def startup() {
    if(config.enableZookeeper) {
      kafkaZookeeper.registerBrokerInZk()
      for (topic <- getAllTopics)
        kafkaZookeeper.registerTopicInZk(topic)
      startupLatch.countDown
    }
    logger.info("Starting log flusher every " + config.flushSchedulerThreadRate + " ms with the following overrides " + logFlushIntervalMap)
    logFlusherScheduler.scheduleWithRate(flushAllLogs, 30 * 1000, config.flushSchedulerThreadRate)
  }

  private def awaitStartup() {
    if (config.enableZookeeper)
      startupLatch.await
  }

  def registerNewTopicInZK(topic: String) {
    if (config.enableZookeeper)
      zkActor ! topic 
  }

  /**
   * Create a log for the given topic and the given partition
   */
  private def createLog(topic: String, partition: Int): Log = {
    logCreationLock synchronized {
      val d = new File(logDir, topic + "-" + partition)
      d.mkdirs()
      new Log(d, maxSize, flushInterval)
    }
  }
  

  def chooseRandomPartition(topic: String): Int = {
    random.nextInt(topicPartitionsMap.getOrElse(topic, numPartitions))
  }

  /**
   * Create the log if it does not exist, if it exists just return it
   */
  def getOrCreateLog(topic: String, partition: Int): Log = {
    awaitStartup
    if (partition < 0 || partition >= topicPartitionsMap.getOrElse(topic, numPartitions)) {
      logger.warn("Wrong partition " + partition + " valid partitions (0," +
              (topicPartitionsMap.getOrElse(topic, numPartitions) - 1) + ")")
      throw new WrongPartitionException("wrong partition " + partition)
    }
    var hasNewTopic = false
    var parts = logs.get(topic)
    if (parts == null) {
      val found = logs.putIfNotExists(topic, new Pool[Int, Log])
      if (found == null)
        hasNewTopic = true
      parts = logs.get(topic)
    }
    var log = parts.get(partition)
    if(log == null) {
      log = createLog(topic, partition)
      Utils.registerMBean(new LogStats(log), "kafka:type=kafka.logs." + log.dir.getName)
      val found = parts.putIfNotExists(partition, log)
      if(found != null) {
        // there was already somebody there
        log.close()
        log = found
      }
      else
        logger.info("Created log for '" + topic + "'-" + partition)
    }

    if (hasNewTopic)
      registerNewTopicInZK(topic)
    log
  }
  
  /**
   * Delete any eligible logs. Return the number of segments deleted.
   */
  def cleanupLogs() {
    logger.debug("Beginning log cleanup...")
    val iter = getLogIterator
    var total = 0
    val startMs = time.milliseconds
    while(iter.hasNext) {
      val log = iter.next
      logger.debug("Garbage collecting '" + log.name + "'")
      val toBeDeleted = log.markDeletedWhile(startMs - _.file.lastModified > this.logCleanupMinAgeMs)
      for(segment <- toBeDeleted) {
        logger.info("Deleting log segment " + segment.file.getName() + " from " + log.name)
        Utils.swallow(logger.warn, segment.messageSet.close())
        if(!segment.file.delete())
          logger.warn("Delete failed.")
        else
          total += 1
      }
    }
    logger.debug("Log cleanup completed. " + total + " files deleted in " + 
                 (time.milliseconds - startMs) / 1000 + " seconds")
  }
  
  /**
   * Close all the logs
   */
  def close() {
    logFlusherScheduler.shutdown
    val iter = getLogIterator
    while(iter.hasNext)
      iter.next.close()
    if (config.enableZookeeper) {
      zkActor ! StopActor
      kafkaZookeeper.close
    }
  }
  
  private def getLogIterator(): Iterator[Log] = {
    new IteratorTemplate[Log] {
      val partsIter = logs.values.iterator
      var logIter: Iterator[Log] = null

      override def makeNext(): Log = {
        while (true) {
          if (logIter != null && logIter.hasNext)
            return logIter.next
          if (!partsIter.hasNext)
            return allDone
          logIter = partsIter.next.values.iterator
        }
        // should never reach here
        assert(false)
        return allDone
      }
    }
  }

  private def flushAllLogs() = {
    logger.debug("flushing the high watermark of all logs")
    for (log <- getLogIterator)
    {
      val timeSinceLastFlush = System.currentTimeMillis - log.getLastFlushedTime
      var logFlushInterval = config.defaultFlushIntervalMs
      if(logFlushIntervalMap.contains(log.getTopicName))
        logFlushInterval = logFlushIntervalMap(log.getTopicName)
      logger.debug(log.getTopicName + " flush interval  " + logFlushInterval +
        " last flushed " + log.getLastFlushedTime + " timesincelastFlush: " + timeSinceLastFlush)
      if(timeSinceLastFlush >= logFlushInterval)
        log.flush
    }
  }


  def getAllTopics(): Iterator[String] = logs.keys.iterator
  def getTopicPartitionsMap() = topicPartitionsMap
}
