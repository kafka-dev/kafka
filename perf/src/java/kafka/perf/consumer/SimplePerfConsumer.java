package kafka.perf.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import scala.collection.Iterator;

import kafka.api.FetchRequest;
import kafka.api.MultiFetchResponse;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;

public class SimplePerfConsumer extends Thread
{
  SimpleConsumer simpleConsumer;
  String topic;
  String consumerName;
  int fetchSize;
  private AtomicLong bytesRec;
  private AtomicLong messagesRec;
  private AtomicLong lastReportMessageRec;
  private AtomicLong lastReportBytesRec;
  private long offset = 0;
  private final int part;

  public SimplePerfConsumer(String topic, String kafkaServerURL, int kafkaServerPort,
                            int kafkaProducerBufferSize, int connectionTimeOut, int reconnectInterval,
                            int fetchSize, String name, int part)
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
    this.part = part;
  }

  public void run() {
    while(true)
    {
      List<FetchRequest> list = new ArrayList<FetchRequest>();
      FetchRequest req = new FetchRequest(topic, part, offset, fetchSize);
      list.add(req);
      MultiFetchResponse response = simpleConsumer.multifetch(list);
      if(response.hasNext())
      {
        ByteBufferMessageSet messages = response.next();
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
    double val = (double)messagesRec.getAndSet(0) / (System.currentTimeMillis() - lastReportMessageRec.getAndSet(System.currentTimeMillis()));
    return val * 1000;
  }

  public String getConsumerName()
  {
    return consumerName;
  }

  public double getMBytesRecPs()
  {
    double val = ((double)bytesRec.getAndSet(0) / (System.currentTimeMillis() - lastReportBytesRec.getAndSet(System.currentTimeMillis()))) / (1024*1024);
    return val * 1000;
  }

}
