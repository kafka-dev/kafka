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

import scala.reflect.BeanProperty
import org.apache.log4j.Logger
import kafka.log.LogManager
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kafka.utils.{Utils, SystemTime, KafkaScheduler}
import kafka.network.{SocketServerStats, SocketServer}

class KafkaServer(val config: KafkaConfig) {
  
  val isStarted = new AtomicBoolean(false)
  
  private val logger = Logger.getLogger(classOf[KafkaServer])
  private val shutdownLatch = new CountDownLatch(1)
  private val statsMBeanName = "kafka:type=kafka.SocketServerStats"
  
  @BeanProperty
  var socketServer: SocketServer = null
  
  @BeanProperty
  val scheduler = new KafkaScheduler(1, "kafka-logcleaner-", false)
  
  private val logManager: LogManager = new LogManager(config,
                                                      scheduler,
                                                      SystemTime,
                                                      1000 * 60 * config.logCleanupIntervalMinutes,
                                                      1000 * 60 * 60 * config.logRetentionHours)

  def startup() {
    try {
      logger.info("Starting Kafka server...")
    
      val handlers = new KafkaRequestHandlers(logManager)
      socketServer = new SocketServer(config.port,
                                      config.numThreads,
                                      config.monitoringPeriodSecs,
                                      handlers.handlerFor)
      Utils.swallow(logger.warn, Utils.registerMBean(socketServer.stats, statsMBeanName))
      socketServer.startup
      /**
       *  Registers this broker in ZK. After this, consumers can connect to broker.
        *  So this should happen after socket server start.
        */
      logManager.startup
      logger.info("Server started.")
    }
    catch {
      case e =>
        logger.fatal(e)
        logger.fatal(Utils.stackTrace(e))
        shutdown
    }
  }
  
  def shutdown() {
    logger.info("Shutting down...")
    scheduler.shutdown
    socketServer.shutdown()
    Utils.swallow(logger.warn, Utils.unregisterMBean(statsMBeanName))
    logManager.close()
    shutdownLatch.countDown()
    logger.info("shut down completed")
  }
  
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def getStats(): SocketServerStats = socketServer.stats
}
