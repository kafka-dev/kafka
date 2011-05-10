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

import kafka.message.ByteBufferMessageSet
import collection.mutable.HashMap
import collection.mutable.Map
import org.apache.log4j.Logger
import kafka.api.ProducerRequest
import kafka.serializer.Encoder
import kafka.producer.SyncProducer
import java.util.Properties

private[async] class EventHandler[T](val producer: SyncProducer,
                                     val serializer: Encoder[T],
                                     val cbkHandler: CallbackHandler[T]) extends IEventHandler[T] {

  private val logger = Logger.getLogger(classOf[EventHandler[T]])

  override def init(props: Properties) { }

  override def handle(events: Seq[QueueItem[T]], syncProducer: SyncProducer) {
    var processedEvents = events
    if(cbkHandler != null)
      processedEvents = cbkHandler.beforeSendingData(events)
    send(serialize(collate(processedEvents)), syncProducer)
  }

  private def send(messagesPerTopic: Map[(String, Int), ByteBufferMessageSet], syncProducer: SyncProducer) {
    if(messagesPerTopic.size > 0) {
      val requests = messagesPerTopic.map(f => new ProducerRequest(f._1._1, f._1._2, f._2)).toArray
      syncProducer.multiSend(requests)
      if(logger.isDebugEnabled)
        logger.debug("kafka producer sent messages for topics " + messagesPerTopic)
    }
  }

  private def serialize(eventsPerTopic: Map[(String,Int), Seq[T]]): Map[(String, Int), ByteBufferMessageSet] = {
    import scala.collection.JavaConversions._
    val eventsPerTopicMap = eventsPerTopic.map(e => ((e._1._1, e._1._2) , e._2.map(l => serializer.toMessage(l))))
    eventsPerTopicMap.map(e => ((e._1._1, e._1._2) , new ByteBufferMessageSet(e._2: _*)))
  }

  private def collate(events: Seq[QueueItem[T]]): Map[(String,Int), Seq[T]] = {
    val collatedEvents = new HashMap[(String, Int), Seq[T]]
    val distinctTopics = events.map(e => e.getTopic).toSeq.distinct
    val distinctPartitions = events.map(e => e.getPartition).distinct

    var remainingEvents = events
    distinctTopics foreach { topic =>
      val topicEvents = remainingEvents partition (e => e.getTopic.equals(topic))
      remainingEvents = topicEvents._2
      distinctPartitions.foreach { p =>
        val topicPartitionEvents = topicEvents._1 partition (e => (e.getPartition == p))
        logger.info("Extracted events " + topicPartitionEvents._1.toString + " for (" + topic + "," + p)
        logger.info("Remaining events " + topicPartitionEvents._2.toString)
        collatedEvents += ( (topic, p) -> topicPartitionEvents._1.map(q => q.getData).toSeq)
      }
    }
    collatedEvents
  }

  override def close = {
    producer.close
  }
}