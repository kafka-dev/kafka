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
package kafka.examples;

import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.SyncProducer;
import kafka.producer.SyncProducerConfig;
import java.util.Properties;
import java.util.ArrayList;
import java.util.List;

public class Producer extends Thread
{
  private final SyncProducer producer;
  private final String topic;
  private final Properties props = new Properties();

  public Producer(String topic)
  {
    props.put("host", KafkaProperties.kafkaServerURL);
    props.put("port", String.valueOf(KafkaProperties.kafkaServerPort));
    props.put("buffer.size", String.valueOf(KafkaProperties.kafkaProducerBufferSize));
    props.put("connect.timeout.ms", String.valueOf(KafkaProperties.connectionTimeOut));
    props.put("reconnect.interval", String.valueOf(KafkaProperties.reconnectInterval));
    producer = new SyncProducer(new SyncProducerConfig(props));
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
