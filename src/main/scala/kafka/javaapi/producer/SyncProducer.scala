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

import kafka.producer.SyncProducerConfig
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.Implicits._

class SyncProducer(val config: SyncProducerConfig) {
  private val underlying = new kafka.producer.SyncProducer(config)

  def send(topic: String, partition: Int, messages: ByteBufferMessageSet) {
    underlying.send(topic, partition, messages)
  }

  def send(topic: String, messages: ByteBufferMessageSet): Unit = send(topic,
                                                                       kafka.api.ProducerRequest.RandomPartition,
                                                                       messages)

  def multiSend(produces: Array[kafka.javaapi.ProducerRequest]) {
    val produceRequests = new Array[kafka.api.ProducerRequest](produces.length)
    for(i <- 0 until produces.length)
      produceRequests(i) = new kafka.api.ProducerRequest(produces(i).topic, produces(i).partition, produces(i).messages)
    underlying.multiSend(produceRequests)
  }

  def close() {
    underlying.close
  }
}