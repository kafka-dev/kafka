package kafka.perf;

public interface KafkaSimulatorMXBean
{
  public String getMBytesSentPs();
  public String getMessagesSentPs();
  public String getProducers();
  public String getMBytesRecPs();
  public String getMessagesRecPs();
  public String getConsumers();
}
