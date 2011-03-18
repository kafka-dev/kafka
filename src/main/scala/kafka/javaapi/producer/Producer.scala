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

package kafka.javaapi.producer

import kafka.utils.Utils
import kafka.producer.async.QueueItem
import java.util.Properties
import kafka.producer.{ProducerPool, ProducerConfig, Partitioner}
import kafka.javaapi.Implicits._

class Producer[K,V](config: ProducerConfig,
                    partitioner: Partitioner[K],
                    producerPool: ProducerPool[V],
                    populateProducerPool: Boolean = true) {

  private val underlying = new kafka.producer.Producer[K,V](config, partitioner, producerPool, populateProducerPool)

  def this(config: ProducerConfig) = this(config, Utils.getObject(config.partitionerClass),
    new ProducerPool[V](config, Utils.getObject(config.serializerClass)))

  def this(config: ProducerConfig,
           eventHandler: kafka.javaapi.producer.async.IEventHandler[V],
           cbkHandler: kafka.javaapi.producer.async.CallbackHandler[V],
           partitioner: Partitioner[K]) = {
    this(config, partitioner,
         new ProducerPool[V](config, Utils.getObject(config.serializerClass),
                             new kafka.producer.async.IEventHandler[V] {
                               override def init(props: Properties) { eventHandler.init(props) }
                               override def handle(events: Seq[QueueItem[V]], producer: kafka.producer.SyncProducer) {
                                 import collection.JavaConversions._
                                 eventHandler.handle(asList(events), producer)
                               }
                               override def close { eventHandler.close }
                             },
                             new kafka.producer.async.CallbackHandler[V] {
                               override def init(props: Properties) { cbkHandler.init(props)}
                               override def beforeEnqueue(data: QueueItem[V] = null.asInstanceOf[QueueItem[V]]): QueueItem[V] = {
                                 cbkHandler.beforeEnqueue(data)
                               }
                               override def afterEnqueue(data: QueueItem[V] = null.asInstanceOf[QueueItem[V]], added: Boolean) {
                                 cbkHandler.afterEnqueue(data, added)
                               }
                               override def afterDequeuingExistingData(data: QueueItem[V] = null): scala.collection.mutable.Seq[QueueItem[V]] = {
                                 import collection.JavaConversions._
                                 cbkHandler.afterDequeuingExistingData(data)
                               }
                               override def beforeSendingData(data: Seq[QueueItem[V]] = null): scala.collection.mutable.Seq[QueueItem[V]] = {
                                 import collection.JavaConversions._
                                 asList(cbkHandler.beforeSendingData(asList(data)))
                               }
                               override def close { cbkHandler.close }
                             }))
  }

  def send(producerData: kafka.javaapi.producer.ProducerData[K,V]) {
    import collection.JavaConversions._
    underlying.send(new kafka.producer.ProducerData[K,V](producerData.getTopic, producerData.getKey,
                                                         asBuffer(producerData.getData)))
  }

  def send(producerData: java.util.List[kafka.javaapi.producer.ProducerData[K,V]]) {
    import collection.JavaConversions._
    underlying.send(asBuffer(producerData).map(pd => new kafka.producer.ProducerData[K,V](pd.getTopic, pd.getKey,
                                                         asBuffer(pd.getData))): _*)
  }

  def close = underlying.close
}