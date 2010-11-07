package kafka.perf.producer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.producer.KafkaProducer;

public class Producer extends Thread
{
  private final KafkaProducer producer;
  private final String topic;
  private final int messageSize;
  private AtomicLong bytesSent =  new AtomicLong(0L);
  private AtomicLong messagesSent =  new AtomicLong(0L);
  private AtomicLong lastReportMessageSent = new AtomicLong(System.currentTimeMillis());
  private AtomicLong lastReportBytesSent = new AtomicLong(System.currentTimeMillis());
  private String procudername;
  private int batchSize;
  private int numParts;
  

  public Producer(String topic, String kafkaServerURL, int kafkaServerPort,
                  int kafkaProducerBufferSize, int connectionTimeOut, int reconnectInterval,
                  int messageSize, String name, int batchSize, int numParts)
  {
    producer = new KafkaProducer(kafkaServerURL,
                                 kafkaServerPort,
                                 kafkaProducerBufferSize,
                                 connectionTimeOut,
                                 reconnectInterval);
    this.topic = topic; 
    this.messageSize = messageSize;
    procudername = name;
    this.batchSize = batchSize;
    this.numParts = numParts;

  }

  public void run() {
    Random random = new Random();
    while(true)
    {
      List<Message> messageList = new ArrayList<Message>();
      for(int i = 0; i < batchSize; i++)
      {
        Message message = new Message(new byte[messageSize]);
        messageList.add(message);
      }
      ByteBufferMessageSet set = new ByteBufferMessageSet(messageList);
      producer.send(topic, random.nextInt(numParts), set);
      bytesSent.getAndAdd(batchSize * messageSize);
      messagesSent.getAndAdd(messageList.size());
    }
  }

  public double getMessagesSentPs()
  {
    double val = (double)messagesSent.getAndSet(0) / (System.currentTimeMillis() - lastReportMessageSent.getAndSet(System.currentTimeMillis()));
    return val * 1000;
  }

  public String getProducerName()
  {
    return procudername;
  }

  public double getMBytesSentPs()
  {
    double val = ((double)bytesSent.getAndSet(0) / (System.currentTimeMillis() - lastReportBytesSent.getAndSet(System.currentTimeMillis()))) / (1024*1024);
    return val * 1000;
  }
}
