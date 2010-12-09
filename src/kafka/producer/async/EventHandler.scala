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

package kafka.producer.async
                                
import kafka.serializer.SerDeser
import kafka.message.ByteBufferMessageSet
import collection.mutable.HashMap
import collection.mutable.Map
import org.apache.log4j.Logger
import kafka.api.ProducerRequest
import kafka.producer.SimpleProducer

class EventHandler[T](val producer: SimpleProducer,
                      val serializer: SerDeser[T]) {

  private val logger = Logger.getLogger(classOf[EventHandler[T]])
  
  def handle(events: Seq[T]) {
    send(serialize(collate(events)))        
  }

  def send(messagesPerTopic: Map[String, ByteBufferMessageSet]) {
    if(messagesPerTopic.size > 0) {
      val requests = messagesPerTopic.map(f => new ProducerRequest(f._1, ProducerRequest.RandomPartition, f._2)).toArray
      producer.multiSend(requests)
      if(logger.isDebugEnabled)
        logger.debug("kafka producer sent messages for topics " + messagesPerTopic)
    }
  }

  def serialize(eventsPerTopic: Map[String, Seq[T]]): Map[String, ByteBufferMessageSet] = {
    import scala.collection.JavaConversions._
    val eventsPerTopicMap = eventsPerTopic.map(e => (e._1, e._2.map(l => serializer.toMessage(l))))
    eventsPerTopicMap.map(e => (e._1, new ByteBufferMessageSet(asList(e._2))))
  }

  def collate(events: Seq[T]): Map[String, Seq[T]] = {
    val collatedEvents = new HashMap[String, Seq[T]]
    val distinctTopics = events.map(e => serializer.getTopic(e)).distinct

    var remainingEvents = events
    distinctTopics foreach { topic => 
      val topicEvents = remainingEvents partition (serializer.getTopic(_).equals(topic))
      remainingEvents = topicEvents._2
      collatedEvents += (topic -> topicEvents._1)
    }
    collatedEvents
  }
  
  def close = {
    producer.close
  }
}