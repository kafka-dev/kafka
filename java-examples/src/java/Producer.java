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

import java.util.ArrayList;
import java.util.List;

import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.KafkaProducer;

public class Producer extends Thread
{
  private final KafkaProducer producer;
  private final String topic;
  
  public Producer(String topic)
  {
    producer = new KafkaProducer(KafkaProperties.kafkaServerURL,
                                 KafkaProperties.kafkaServerPort,
                                 KafkaProperties.kafkaProducerBufferSize,
                                 KafkaProperties.connectionTimeOut,
                                 KafkaProperties.reconnectInterval);
    this.topic = topic; 
    
  }
  
  public void run() {
    int messageNo = 1;
    while(true)
    {
      String messageStr = new String("Message_" + messageNo);
      Message message = new Message(messageStr.getBytes());
      List<Message> messageList = new ArrayList<Message>();
      messageList.add(message);
      ByteBufferMessageSet set = new ByteBufferMessageSet(messageList);
      producer.send(topic, set);
      messageNo++;
    }
  }

}
