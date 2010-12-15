package kafka.perf.consumer;

public interface PerfConsumer {
	public String getConsumerName();
	public double getMessagesRecPs();  
	public double getMBytesRecPs();
}
