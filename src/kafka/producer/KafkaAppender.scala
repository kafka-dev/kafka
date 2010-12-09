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

package kafka.producer

import async.MissingConfigException
import org.apache.log4j.spi.LoggingEvent
import kafka.message.ByteBufferMessageSet
import java.util.Date
import org.apache.log4j.{Logger, AppenderSkeleton}
import kafka.utils.Utils
import kafka.serializer.Encoder

class KafkaAppender extends AppenderSkeleton {
  var port:Int = 0
  var host:String = null
  var topic:String = null
  var encoderClass:String = null
  
  private var producer:SimpleProducer = null
  private val logger = Logger.getLogger(classOf[KafkaAppender])
  private var encoder: Encoder[AnyRef] = null
  
  def getPort:Int = port
  def setPort(port: Int) = { this.port = port }

  def getHost:String = host
  def setHost(host: String) = { this.host = host }

  def getTopic:String = topic
  def setTopic(topic: String) = { this.topic = topic }

  def getEncoder:String = encoderClass
  def setEncoder(encoder: String) = { this.encoderClass = encoder }
  
  override def activateOptions = {
    // check for config parameter validity
    if(host == null)
      throw new MissingConfigException("Broker Host must be specified by the Kafka log4j appender")
    if(port == 0)
      throw new MissingConfigException("Broker Port must be specified by the Kafka log4j appender") 
    if(topic == null)
      throw new MissingConfigException("topic must be specified by the Kafka log4j appender")
    if(encoderClass == null)
      throw new MissingConfigException("Encoder must be specified by the Kafka log4j appender")
    // instantiate the encoder, if present
    encoder = Utils.getObject(encoderClass)    
    producer = new SimpleProducer(host, port, 100*1024, 30000, 10000)
    logger.info("Kafka producer connected to " + host + "," + port)
    logger.info("Logging for topic: " + topic)
  }
  
  override def append(event: LoggingEvent) = {
    if (logger.isDebugEnabled){
      logger.debug("[" + new Date(event.getTimeStamp).toString + "]" + event.getRenderedMessage +
            " for " + host + "," + port)
    }
    val message = encoder.toMessage(event)
    producer.send(topic, new ByteBufferMessageSet(message))
  }

  override def close = {
    if(!this.closed) {
      this.closed = true
      producer.close
    }
  }

  override def requiresLayout: Boolean = false
}
