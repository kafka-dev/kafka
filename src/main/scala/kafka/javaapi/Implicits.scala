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
package kafka.javaapi

import kafka.producer.ProducerPool
import kafka.producer.async.QueueItem
import java.nio.ByteBuffer

private[javaapi] object Implicits {
  implicit def javaMessageSetToScalaMessageSet(messageSet: kafka.javaapi.message.ByteBufferMessageSet):
     kafka.message.ByteBufferMessageSet = messageSet.underlying

  implicit def scalaMessageSetToJavaMessageSet(messageSet: kafka.message.ByteBufferMessageSet):
     kafka.javaapi.message.ByteBufferMessageSet = {
    new kafka.javaapi.message.ByteBufferMessageSet(messageSet.buffer, messageSet.errorCOde)
  }

  implicit def toJavaSyncProducer(producer: kafka.producer.SyncProducer): kafka.javaapi.producer.SyncProducer = {
    new kafka.javaapi.producer.SyncProducer(producer.config)
  }

  implicit def toSyncProducer(producer: kafka.javaapi.producer.SyncProducer): kafka.producer.SyncProducer = {
    new kafka.producer.SyncProducer(producer.config)
  }

  implicit def toScalaEventHandler[T](eventHandler: kafka.javaapi.producer.async.IEventHandler[T])
       : kafka.producer.async.IEventHandler[T] = {
    new kafka.producer.async.IEventHandler[T] {
      override def init(props: java.util.Properties) { eventHandler.init(props) }
      override def handle(events: Seq[QueueItem[T]], producer: kafka.producer.SyncProducer) {
        import collection.JavaConversions._
        eventHandler.handle(asList(events), producer)
      }
      override def close { eventHandler.close }
    }
  }

  implicit def toJavaEventHandler[T](eventHandler: kafka.producer.async.IEventHandler[T])
    : kafka.javaapi.producer.async.IEventHandler[T] = {
    new kafka.javaapi.producer.async.IEventHandler[T] {
      override def init(props: java.util.Properties) { eventHandler.init(props) }
      override def handle(events: java.util.List[QueueItem[T]], producer: kafka.javaapi.producer.SyncProducer) {
        import collection.JavaConversions._
        eventHandler.handle(asBuffer(events), producer)
      }
      override def close { eventHandler.close }
    }
  }

  implicit def toScalaCbkHandler[T](cbkHandler: kafka.javaapi.producer.async.CallbackHandler[T])
      : kafka.producer.async.CallbackHandler[T] = {
    new kafka.producer.async.CallbackHandler[T] {
      override def init(props: java.util.Properties) { cbkHandler.init(props)}
      override def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T] = {
        cbkHandler.beforeEnqueue(data)
      }
      override def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean) {
        cbkHandler.afterEnqueue(data, added)
      }
      override def afterDequeuingExistingData(data: QueueItem[T] = null): scala.collection.mutable.Seq[QueueItem[T]] = {
        import collection.JavaConversions._
        cbkHandler.afterDequeuingExistingData(data)
      }
      override def beforeSendingData(data: Seq[QueueItem[T]] = null): scala.collection.mutable.Seq[QueueItem[T]] = {
        import collection.JavaConversions._
        asList(cbkHandler.beforeSendingData(asList(data)))
      }
      override def close { cbkHandler.close }
    }
  }

  implicit def toJavaCbkHandler[T](cbkHandler: kafka.producer.async.CallbackHandler[T])
      : kafka.javaapi.producer.async.CallbackHandler[T] = {
    new kafka.javaapi.producer.async.CallbackHandler[T] {
      override def init(props: java.util.Properties) { cbkHandler.init(props)}
      override def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T] = {
        cbkHandler.beforeEnqueue(data)
      }
      override def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean) {
        cbkHandler.afterEnqueue(data, added)
      }
      override def afterDequeuingExistingData(data: QueueItem[T] = null)
      : java.util.List[QueueItem[T]] = {
        import collection.JavaConversions._
        asList(cbkHandler.afterDequeuingExistingData(data))
      }
      override def beforeSendingData(data: java.util.List[QueueItem[T]] = null)
      : java.util.List[QueueItem[T]] = {
        import collection.JavaConversions._
        asBuffer(cbkHandler.beforeSendingData(asBuffer(data)))
      }
      override def close { cbkHandler.close }
    }
  }

  implicit def toMultiFetchResponse(response: kafka.javaapi.MultiFetchResponse): kafka.api.MultiFetchResponse =
    response.underlying

  implicit def toJavaMultiFetchResponse(response: kafka.api.MultiFetchResponse): kafka.javaapi.MultiFetchResponse =
    new kafka.javaapi.MultiFetchResponse(response.buffer, response.numSets)
}