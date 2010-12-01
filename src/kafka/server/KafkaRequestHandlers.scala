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

import java.nio.channels._
import org.apache.log4j.Logger
import kafka.producer._
import kafka.consumer._
import kafka.log._
import kafka.network._
import kafka.message._
import kafka.server._
import kafka.api._
import kafka.common.{WrongPartitionException, ErrorMapping}
import kafka.utils.SystemTime

/**
 * Logic to handle the various Kafka requests
 */
class KafkaRequestHandlers(val logManager: LogManager) {
  
  private val logger = Logger.getLogger(classOf[KafkaRequestHandlers])
  
  def handlerFor(requestTypeId: Short, request: Receive): Handler.Handler = {
    requestTypeId match {
      case RequestKeys.Produce => handleProducerRequest _
      case RequestKeys.Fetch => handleFetchRequest _
      case RequestKeys.MultiFetch => handleMultiFetchRequest _
      case RequestKeys.MultiProduce => handleMultiProducerRequest _
      case RequestKeys.Offsets => handleOffsetRequest _
      case _ => throw new IllegalStateException("No mapping found for handler id " + requestTypeId)
    }
  }
  
  def handleProducerRequest(receive: Receive): Option[Send] = {
    val sTime = SystemTime.milliseconds
    if(logger.isTraceEnabled)
      logger.trace("Handling producer request")
    val request = ProducerRequest.readFrom(receive.buffer)
    val partition = request.getTranslatedPartition(logManager.chooseRandomPartition)
    try {
      logManager.getOrCreateLog(request.topic, partition).append(request.messages)
      if(logger.isTraceEnabled)
        logger.trace(request.messages.sizeInBytes + " bytes written to logs.")
    }
    catch {
      case e: WrongPartitionException => // let it go for now
    }
    if (logger.isDebugEnabled)
      logger.debug("kafka produce time " + (SystemTime.milliseconds - sTime) + " ms")
    None
  }
  
  def handleMultiProducerRequest(receive: Receive): Option[Send] = {
    if(logger.isTraceEnabled)
      logger.trace("Handling multiproducer request")
    val request = MultiProducerRequest.readFrom(receive.buffer)
    try {
      for (produce <- request.produces) {
        val partition = produce.getTranslatedPartition(logManager.chooseRandomPartition)
        logManager.getOrCreateLog(produce.topic, partition).append(produce.messages)
        if(logger.isTraceEnabled)
          logger.trace(produce.messages.sizeInBytes + " bytes written to logs.")
      }
    }
    catch {
      case e: WrongPartitionException => // let it go for now
    }
    None
  }

  def handleFetchRequest(request: Receive): Option[Send] = {
    if(logger.isTraceEnabled)
      logger.trace("Handling fetch request")
    val fetchRequest = FetchRequest.readFrom(request.buffer)
    Some(readMessageSet(fetchRequest))
  }
  
  def handleMultiFetchRequest(request: Receive): Option[Send] = {
    if(logger.isTraceEnabled)
      logger.trace("Handling multifetch request")
    val multiFetchRequest = MultiFetchRequest.readFrom(request.buffer)
    var responses = multiFetchRequest.fetches.map(fetch =>
        readMessageSet(fetch)).toList
    
    Some(new MultiMessageSetSend(responses))
  }

  private def readMessageSet(fetchRequest: FetchRequest): MessageSetSend = {
    var  response: MessageSetSend = null
    try {
      val log = logManager.getOrCreateLog(fetchRequest.topic, fetchRequest.partition)
      response = new MessageSetSend(log.read(fetchRequest.offset, fetchRequest.maxSize))
    }
    catch {
      case e: RuntimeException =>
        response=new MessageSetSend(MessageSet.Empty, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Exception]]))
      case e2 => throw e2
    }
    response
  }

  def handleOffsetRequest(request: Receive): Option[Send] = {
    if(logger.isTraceEnabled)
      logger.trace("Handling offset request")
    val offsetRequest = OffsetRequest.readFrom(request.buffer)
    val log = logManager.getOrCreateLog(offsetRequest.topic, offsetRequest.partition)
    val offsets = log.getOffsetsBefore(offsetRequest)
    val response = new OffsetArraySend(offsets)
    Some(response)
  }
}
