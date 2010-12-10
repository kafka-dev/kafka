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

package kafka.integration

import junit.framework._
import kafka.consumer.SimpleConsumer
import kafka.producer.SimpleProducer

trait ProducerConsumerTestHarness extends TestCase {
  
    val port: Int
    val host = "localhost"
    var producer: SimpleProducer = null
    var consumer: SimpleConsumer = null
    
    override def setUp() {
      producer = new SimpleProducer(host,
                                   port,
                                   64*1024,
                                   100000,
                                   10000)
      consumer = new SimpleConsumer(host,
                                   port,
                                   1000000,
                                   64*1024)
      super.setUp()
    }
    
    override def tearDown() {
      producer.close()
      consumer.close()
      super.tearDown()
    }
}
