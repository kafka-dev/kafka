/**
 * 
 */
package kafka.perf.producer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import scala.collection.Iterator;

import kafka.api.FetchRequest;
import kafka.api.MultiFetchResponse;
import kafka.api.OffsetRequest;
import kafka.consumer.SimpleConsumer;
import kafka.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageSet;
import kafka.producer.SimpleProducer;

/**
 * @author nnarkhed
 *
 */
public class PerfProducer extends Thread implements Producer {

	private final SimpleProducer producer;
	private final String topic;
	private AtomicLong bytesSent =  new AtomicLong(0L);
	private AtomicLong totalBytesSent =  new AtomicLong(0L);
	private AtomicLong messagesSent =  new AtomicLong(0L);
	private AtomicLong lastReportMessageSent = new AtomicLong(System.currentTimeMillis());
	private AtomicLong lastReportBytesSent = new AtomicLong(System.currentTimeMillis());
	private String producerName;
	private int batchSize;
	private int numParts;
	private boolean compression;

	/* consumption */
	private Map<Integer, Long> offsetMap = new HashMap<Integer, Long>();
	private SimpleConsumer simpleConsumer;
	private int fetchSize;
	private final String consumerTopic;

	public PerfProducer(String topic, String kafkaServerURL, int kafkaServerPort,
			int kafkaProducerBufferSize, int connectionTimeOut, int reconnectInterval,
			String name, int batchSize, int numParts, boolean compression,
			int fetchSize, String consumerKafkaServer, int consumerKafkaPort, String consumerTopic)
	{
		simpleConsumer = new SimpleConsumer(consumerKafkaServer,
				consumerKafkaPort,
				connectionTimeOut,
				kafkaProducerBufferSize);
		this.fetchSize = fetchSize;
		this.consumerTopic = consumerTopic;

		producer = new SimpleProducer(kafkaServerURL,
				kafkaServerPort,
				kafkaProducerBufferSize,
				connectionTimeOut,
				reconnectInterval);
		this.topic = topic; 
		producerName = name;
		this.batchSize = batchSize;
		this.numParts = numParts;
		this.compression = compression;
		for(int i = 0; i < numParts;i ++) {
			long[] offset = simpleConsumer.getOffsetsBefore(consumerTopic, i, OffsetRequest.EARLIEST_TIME(), 1);
			System.out.println("Starting offset =" + offset[0]);
			offsetMap.put(i, offset[0]);
		}

	}

	public void run() {
		while(true)
		{
			List<FetchRequest> list = new ArrayList<FetchRequest>();
			for(int i=0 ; i < numParts; i++)
			{
				FetchRequest req = new FetchRequest(consumerTopic, i, offsetMap.get(i), fetchSize);
				list.add(req);
				//				System.out.println("Fetch request = " + req.toString());
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

				Random random = new Random();
				Iterator<Message> it =  messages.iterator();
				while(it.hasNext()) {
					int bytesCount = 0;
					List<Message> messageList = new ArrayList<Message>();
					for(int j = 0; j < batchSize && it.hasNext(); j++)
					{
						Message message = it.next();
						messageList.add(message);
						bytesCount += MessageSet.entrySize(message);
					}
					if(messageList.size() > 0) {
						ByteBufferMessageSet set = new ByteBufferMessageSet(messageList, compression);
						producer.send(topic, numParts == 1 ? 0:random.nextInt(numParts), set);
						bytesSent.getAndAdd(bytesCount);
						messagesSent.getAndAdd(messageList.size());
					}
				}
			}
			totalBytesSent.addAndGet(bytesSent.get());
			System.out.println("Total bytes sent : " + bytesSent.get());
		}
	}

	/* (non-Javadoc)
	 * @see kafka.perf.producer.Producer#getMBytesSentPs()
	 */
	public double getMBytesSentPs() {
		System.out.println("Calling getMBytesSentPs(): total bytes = " + bytesSent.get());
		double timeWindow = (System.currentTimeMillis() - lastReportBytesSent.get())/(double)1000;
		System.out.println("Time window : " + timeWindow + "s");
		double val = bytesSent.get()/timeWindow;
		val = val/((double)(1024*1024));
		return val;
	}

	/* (non-Javadoc)
	 * @see kafka.perf.producer.Producer#getMessagesSentPs()
	 */
	public double getMessagesSentPs() {
		double val = (double)messagesSent.get() / (System.currentTimeMillis() - lastReportMessageSent.get());
		return val * 1000;
	}

	/* (non-Javadoc)
	 * @see kafka.perf.producer.Producer#getProducerName()
	 */
	public String getProducerName() {
		return producerName;
	}

	public long getTotalBytesSent() {
		return bytesSent.get();
	}

}
