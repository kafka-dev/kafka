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

package kafka.perf.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import kafka.api.FetchRequest;
import kafka.api.MultiFetchResponse;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import scala.collection.Iterator;

public class SimplePerfConsumer extends Thread implements PerfConsumer
{
  private SimpleConsumer simpleConsumer;
  private String topic;
  private String consumerName;
  private int fetchSize;
  private AtomicLong bytesRec;
  private AtomicLong messagesRec;
  private AtomicLong lastReportMessageRec;
  private AtomicLong lastReportBytesRec;
  private Map<Integer, Long> offsetMap = new HashMap<Integer, Long>();
  private final int numParts;

  public SimplePerfConsumer(String topic, String kafkaServerURL, int kafkaServerPort,
                            int kafkaProducerBufferSize, int connectionTimeOut, int reconnectInterval,
                            int fetchSize, String name, int numParts)
  {
    simpleConsumer = new SimpleConsumer(kafkaServerURL,
                                        kafkaServerPort,
                                        connectionTimeOut,
                                        kafkaProducerBufferSize);
    this.topic = topic; 
    this.fetchSize = fetchSize;
    consumerName = name;
    bytesRec =  new AtomicLong(0L);
    messagesRec =  new AtomicLong(0L);
    lastReportMessageRec = new AtomicLong(System.currentTimeMillis());
    lastReportBytesRec = new AtomicLong(System.currentTimeMillis());
    this.numParts = numParts;
    for(int i = 0; i < numParts;i ++)
      offsetMap.put(i, 0L);
  }

  public void run() {
    while(true)
    {
      List<FetchRequest> list = new ArrayList<FetchRequest>();
      for(int i=0 ; i < numParts; i++)
      {
        FetchRequest req = new FetchRequest(topic, i, offsetMap.get(i), fetchSize);
        list.add(req);
      }
      MultiFetchResponse response = simpleConsumer.multifetch(list);
      int i = 0;
      if(response.hasNext())
      {
        ByteBufferMessageSet messages = response.next();
        messages.disableDeepIteration();
        try {
          long offset = offsetMap.get(i);
          offset += messages.validBytes();
          offsetMap.put(i, offset);
        }catch(Exception ioe) {
          System.err.println("Faulty offset: " + offsetMap.get(i));
          System.err.println("Faulty fetch request: " + list.get(i));
            continue;
        }
        bytesRec.getAndAdd(messages.sizeInBytes());

        Iterator<Message> it =  messages.iterator();
        while(it.hasNext())
        {
          it.next();
          messagesRec.getAndIncrement();
        }
        i++;
      }
    }
  }

  public double getMessagesRecPs()
  {
    double val = (double)messagesRec.get() / (System.currentTimeMillis() - lastReportMessageRec.get());
    return val * 1000;
  }

  public String getConsumerName()
  {
    return consumerName;
  }

  public double getMBytesRecPs()
  {
    double val = ((double)bytesRec.get() / (System.currentTimeMillis() - lastReportBytesRec.get())) / (1024*1024);
    return val * 1000;
  }

}
