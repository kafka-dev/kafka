package kafka.perf.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;


import kafka.api.FetchRequest;
import kafka.javaapi.MultiFetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;

public class SimplePerfConsumer extends Thread
{
  private SimpleConsumer simpleConsumer;
  private String topic;
  private String consumerName;
  private int fetchSize;
  private AtomicLong bytesRec;
  private AtomicLong messagesRec;
  private AtomicLong lastReportMessageRec;
  private AtomicLong lastReportBytesRec;
  private long offset = 0;
  private final int numParts;

  public SimplePerfConsumer(String topic, String kafkaServerURL, int kafkaServerPort,
                            int kafkaProducerBufferSize, int connectionTimeOut, int reconnectInterval,
                            int fetchSize, String name, int numParts)
  {
    super(name);
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
  }

  public void run() {
    while(true)
    {
      List<FetchRequest> list = new ArrayList<FetchRequest>();
      for(int i=0 ; i < numParts; i++)
      {
        FetchRequest req = new FetchRequest(topic, i, offset, fetchSize);
        list.add(req);
      }


      MultiFetchResponse response = simpleConsumer.multifetch(list);
      for (ByteBufferMessageSet messages: response)
      {
        offset+= messages.validBytes();
        bytesRec.getAndAdd(messages.sizeInBytes());
        
        Iterator<Message> it =  messages.iterator();
        while(it.hasNext())
        {
          it.next();
          messagesRec.getAndIncrement();
        }
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
